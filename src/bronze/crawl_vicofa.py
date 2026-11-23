"""Incremental VICOFA website crawler -> immutable MinIO Bronze snapshots.

Run from the repository root with:
    python -m src.bronze.crawl_vicofa

The crawler reads the existing MinIO JSONL snapshots as its checkpoint, fetches
only newly discovered article IDs, and writes a new full snapshot pair only when
new records are found. Existing raw objects are never overwritten.
"""
from __future__ import annotations

import argparse
import json
import random
import re
import time
from collections import OrderedDict
from datetime import datetime, timezone
from urllib.parse import parse_qs, urljoin, urlsplit
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from src.bronze.metadata import timestamp_id, upload_bytes_and_metadata
from src.bronze.minio_client import get_bucket_name, get_minio_client
from src.bronze.run_logging import finish_run, save_run, start_run

BASE = "https://vicofa.org.vn/"
LIST_URL = urljoin(BASE, "tin-thi-truong-hang-ngay-b42.html")
SOURCE = "vicofa_website"
DOMAIN = "vicofa"
PREFIX = f"bronze/{DOMAIN}/source={SOURCE}/"
USER_AGENT = "TLCN-Robusta-Lakehouse/1.0 (academic data ingestion)"
CONTENT_SELECTORS = [
    ".noidung_baiviet",
    ".chitiet_baiviet",
    ".baiviet_chitiet",
    ".noidung",
    ".html_content",
    "article",
]
EXPECTED_FIELDS = {
    "article_list.jsonl": {"id", "title", "url", "date", "summary"},
    "articles.jsonl": {
        "id", "title", "url", "date", "summary", "title_detail", "date_detail", "content", "source"
    },
}
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class SourceUnavailable(RuntimeError):
    """The source site temporarily denies or cannot serve the crawl request."""


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            "Referer": BASE,
        }
    )
    return session


def polite_sleep(min_seconds: float, max_seconds: float) -> None:
    if max_seconds > 0:
        time.sleep(random.uniform(min_seconds, max_seconds))


def get_html(
    session: requests.Session,
    url: str,
    *,
    timeout: int = 25,
    retries: int = 4,
) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code == 404:
                return None
            if response.status_code == 403:
                raise SourceUnavailable(f"HTTP 403 Forbidden: {url}")
            if response.status_code in RETRYABLE_STATUS:
                if attempt == retries:
                    raise SourceUnavailable(f"HTTP {response.status_code} after {retries} attempts: {url}")
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(max(float(retry_after), 1.0), 60.0) if retry_after else min(2 ** attempt, 20)
                except ValueError:
                    delay = min(2 ** attempt, 20)
                print(f"RETRY {attempt}/{retries}: HTTP {response.status_code}; waiting {delay:.1f}s", flush=True)
                time.sleep(delay)
                continue
            response.raise_for_status()
            response.encoding = response.apparent_encoding or "utf-8"
            return response.text
        except SourceUnavailable:
            raise
        except requests.RequestException as exc:
            if attempt == retries:
                raise SourceUnavailable(f"Request failed after {retries} attempts: {url}: {exc}") from exc
            delay = min(2 ** attempt, 20)
            print(f"RETRY {attempt}/{retries}: {exc}; waiting {delay}s", flush=True)
            time.sleep(delay)
    return None


def check_robots(session: requests.Session) -> None:
    robots_url = urljoin(BASE, "robots.txt")
    try:
        response = session.get(robots_url, timeout=20)
    except requests.RequestException as exc:
        raise SourceUnavailable(f"Cannot read robots.txt: {exc}") from exc

    if response.status_code == 404:
        print("ROBOTS_TXT: not found (no published crawl rules)", flush=True)
        return
    if response.status_code != 200:
        raise SourceUnavailable(f"robots.txt returned HTTP {response.status_code}; refusing to crawl")

    parser = RobotFileParser()
    parser.set_url(robots_url)
    parser.parse(response.text.splitlines())
    if not parser.can_fetch(USER_AGENT, LIST_URL):
        raise SourceUnavailable(f"robots.txt disallows this crawler for {LIST_URL}")
    print("ROBOTS_TXT: crawl allowed for listing URL", flush=True)


def parse_list(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    for li in soup.select(".baiviet_box ul > li"):
        anchor = li.select_one("h2.tieude a")
        if not anchor or not anchor.get("href"):
            continue
        href = urljoin(BASE, anchor["href"])
        match = re.search(r"-bid(\d+)\.html", href)
        if not match:
            continue
        date_node = li.select_one(".ngay .giatri")
        summary_node = li.select_one(".mota")
        items.append(
            {
                "id": int(match.group(1)),
                "title": anchor.get_text(" ", strip=True),
                "url": href,
                "date": date_node.get_text(" ", strip=True) if date_node else None,
                "summary": summary_node.get_text(" ", strip=True) if summary_node else None,
            }
        )
    return items


def extract_max_page(html: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")
    pages: list[int] = []
    for anchor in soup.select("a[href]"):
        query = parse_qs(urlsplit(urljoin(BASE, anchor["href"])).query)
        for value in query.get("p", []):
            if value.isdigit():
                pages.append(int(value))
    return max(pages) if pages else None


def parse_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "iframe", "noscript"]):
        tag.decompose()

    heading = soup.select_one("h1")
    title = heading.get_text(" ", strip=True) if heading else None
    content_node = None
    for selector in CONTENT_SELECTORS:
        node = soup.select_one(selector)
        if node and len(node.get_text(strip=True)) > 200:
            content_node = node
            break
    if content_node is None:
        best_node = None
        best_length = 0
        for div in soup.find_all("div"):
            text_length = sum(
                len(paragraph.get_text(strip=True))
                for paragraph in div.find_all("p", recursive=False)
            )
            if text_length > best_length:
                best_node, best_length = div, text_length
        content_node = best_node

    date_node = soup.select_one(".ngay .giatri")
    return {
        "title_detail": title,
        "date_detail": date_node.get_text(" ", strip=True) if date_node else None,
        "content": content_node.get_text("\n", strip=True) if content_node else None,
    }


def read_jsonl_object(client, bucket: str, key: str) -> list[dict]:
    response = client.get_object(bucket, key)
    try:
        raw = response.read()
    finally:
        response.close()
        response.release_conn()

    records: list[dict] = []
    for line_no, line in enumerate(raw.decode("utf-8-sig").splitlines(), start=1):
        if not line.strip():
            raise ValueError(f"Blank JSONL line in {key}:{line_no}")
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ValueError(f"JSONL record is not an object in {key}:{line_no}")
        filename = key.rsplit("/", 1)[-1]
        expected = EXPECTED_FIELDS.get(filename)
        if expected is None or set(record) != expected:
            raise ValueError(f"Unexpected VICOFA fields in {key}:{line_no}: {sorted(record)}")
        if not isinstance(record.get("id"), int) or isinstance(record.get("id"), bool):
            raise ValueError(f"Invalid VICOFA article ID in {key}:{line_no}")
        if not str(record.get("url", "")).startswith(BASE):
            raise ValueError(f"Invalid VICOFA article URL in {key}:{line_no}")
        records.append(record)
    return records


def select_complete_snapshot_keys(keys: list[str]) -> list[str]:
    """Keep the first legacy list/detail objects plus only complete later pairs.

    The original VICOFA load stored the list and detail JSONL files in separate
    batch IDs. New crawler runs store both files under one shared batch ID. If a
    process dies between the two uploads, its orphan object is ignored so the
    next run can safely retry from the last complete checkpoint.
    """
    expected = {"article_list.jsonl", "articles.jsonl"}
    first_by_type: dict[str, str] = {}
    batches: dict[str, dict[str, str]] = {}
    for key in sorted(keys):
        filename = key.rsplit("/", 1)[-1]
        if filename not in expected:
            continue
        first_by_type.setdefault(filename, key)
        batch_segment = next(
            (part for part in key.split("/") if part.startswith("batch_id=")),
            None,
        )
        batch_id = batch_segment.split("=", 1)[1] if batch_segment else key.rsplit("/", 2)[0]
        batches.setdefault(batch_id, {})[filename] = key

    if set(first_by_type) != expected:
        raise RuntimeError("VICOFA MinIO checkpoint must contain at least one list and one detail object")

    selected = set(first_by_type.values())
    for files in batches.values():
        if set(files) == expected:
            selected.update(files.values())
    return sorted(selected)


def load_existing(client, bucket: str) -> tuple[OrderedDict[int, dict], OrderedDict[int, dict]]:
    listing: OrderedDict[int, dict] = OrderedDict()
    details: OrderedDict[int, dict] = OrderedDict()
    all_keys = sorted(
        obj.object_name
        for obj in client.list_objects(bucket, prefix=PREFIX, recursive=True)
        if "/raw/" in obj.object_name
        and obj.object_name.endswith(("article_list.jsonl", "articles.jsonl"))
    )
    keys = select_complete_snapshot_keys(all_keys)
    ignored = len(all_keys) - len(keys)
    if ignored:
        print(f"VICOFA_INCOMPLETE_SNAPSHOT_OBJECTS_IGNORED: {ignored}", flush=True)
    for key in keys:
        filename = key.rsplit("/", 1)[-1]
        target = listing if filename == "article_list.jsonl" else details
        for record in read_jsonl_object(client, bucket, key):
            # Keep the first observed version of an article. Later snapshots are
            # append-only checkpoints; they must not rewrite historical Bronze rows.
            target.setdefault(record["id"], record)

    if bool(listing) != bool(details):
        raise RuntimeError("VICOFA MinIO checkpoint is incomplete: list/detail objects are not both present")
    if listing and set(listing) != set(details):
        missing_detail = sorted(set(listing) - set(details))[:10]
        missing_list = sorted(set(details) - set(listing))[:10]
        raise RuntimeError(
            "VICOFA MinIO checkpoint has unpaired article IDs; "
            f"missing_detail={missing_detail}, missing_list={missing_list}"
        )
    return listing, details


def jsonl_bytes(records: list[dict]) -> bytes:
    return "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    ).encode("utf-8")


def upload_full_snapshot(
    listing: list[dict],
    details: list[dict],
    *,
    batch_id: str,
) -> list[dict]:
    common = {
        "domain": DOMAIN,
        "source": SOURCE,
        "source_type": "website",
        "content_type": "application/x-ndjson",
        "batch_id": batch_id,
    }
    results = []
    for filename, records in (
        ("article_list.jsonl", listing),
        ("articles.jsonl", details),
    ):
        results.append(
            upload_bytes_and_metadata(
                raw_bytes=jsonl_bytes(records),
                raw_filename=filename,
                source_url=LIST_URL if filename == "article_list.jsonl" else BASE,
                extra_metadata={
                    "crawl_program": "crawl_vicofa_v2",
                    "snapshot_type": "full_snapshot",
                    "record_count": len(records),
                    "article_id_min": min((record["id"] for record in records), default=None),
                    "article_id_max": max((record["id"] for record in records), default=None),
                    "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                },
                **common,
            )
        )
    return results


def crawl(args) -> dict[str, int | bool]:
    client = get_minio_client()
    bucket = get_bucket_name()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f"MinIO bucket not found: {bucket}")

    existing_list, existing_details = load_existing(client, bucket)
    listing: OrderedDict[int, dict] = OrderedDict(existing_list)
    details: OrderedDict[int, dict] = OrderedDict(existing_details)
    baseline_count = len(listing)
    print(
        f"VICOFA_CHECKPOINT: list={len(existing_list)}, detail={len(existing_details)}, "
        f"known_articles={baseline_count}",
        flush=True,
    )

    session = make_session()
    check_robots(session)

    seen_page_signatures: set[tuple[int, ...]] = set()
    no_new_pages = 0
    pages_scanned = 0
    discovered = 0
    page_limit = args.max_pages

    for page in range(1, page_limit + 1):
        url = LIST_URL if page == 1 else f"{LIST_URL}?p={page}"
        html = get_html(session, url)
        if html is None:
            if page == 1:
                raise SourceUnavailable("VICOFA listing returned HTTP 404")
            print(f"VICOFA_LIST_END: page={page} returned 404", flush=True)
            break

        # parsed = parse_list(html)
        # if not parsed:
        #     if page == 1:
        #         raise RuntimeError("VICOFA listing structure changed: no article cards were parsed")
        parsed = parse_list(html)
        if not parsed:
            if page == 1:
                raise SourceUnavailable(
                    "VICOFA listing returned no article cards. "
                    "Website may have changed its HTML structure "
                    "or returned an unexpected response."
                )
            print(
                f"VICOFA_LIST_END: page={page} has no parsed article cards",
                flush=True
            )
            break

        signature = tuple(record["id"] for record in parsed)
        if signature in seen_page_signatures:
            print(f"VICOFA_LIST_STOP: repeated page signature at page={page}", flush=True)
            break
        seen_page_signatures.add(signature)

        new_on_page = 0
        for record in parsed:
            if record["id"] not in listing:
                listing[record["id"]] = record
                new_on_page += 1
                discovered += 1

        pages_scanned += 1
        print(
            f"VICOFA_PAGE: {page}/{page_limit}, parsed={len(parsed)}, "
            f"new={new_on_page}, total_known={len(listing)}",
            flush=True,
        )

        if baseline_count and new_on_page == 0:
            no_new_pages += 1
        else:
            no_new_pages = 0
        if baseline_count and no_new_pages >= args.stop_after_known_pages:
            print(
                f"VICOFA_INCREMENTAL_STOP: {no_new_pages} consecutive pages had no new IDs",
                flush=True,
            )
            break

        max_page = extract_max_page(html)
        if max_page and page >= max_page:
            break
        polite_sleep(args.delay_min, args.delay_max)
    else:
        raise RuntimeError(
            f"Reached --max-pages={page_limit} without reaching the end; refusing partial snapshot"
        )

    todo = [record for article_id, record in listing.items() if article_id not in details]
    print(f"VICOFA_DETAILS_TODO: {len(todo)} (already complete={len(details)})", flush=True)
    for index, record in enumerate(todo, start=1):
        html = get_html(session, record["url"])
        if not html:
            raise SourceUnavailable(f"Article detail returned no HTML: {record['url']}")
        detail = parse_detail(html)
        complete = dict(record)
        complete.update(detail)
        complete["source"] = "vicofa.org.vn"
        if set(complete) != EXPECTED_FIELDS["articles.jsonl"]:
            raise RuntimeError(f"Unexpected VICOFA detail schema for article {record['id']}")
        details[record["id"]] = complete
        print(
            f"VICOFA_DETAIL: {index}/{len(todo)} id={record['id']} "
            f"content_chars={len(complete.get('content') or '')}",
            flush=True,
        )
        polite_sleep(args.delay_min, args.delay_max)

    if set(listing) != set(details):
        raise RuntimeError("VICOFA list/detail article IDs do not match; no snapshot uploaded")

    new_detail_count = len(details) - len(existing_details)
    uploaded = False
    if discovered or new_detail_count:
        if args.dry_run:
            print(
                f"DRY_RUN_WOULD_UPLOAD: list={len(listing)}, detail={len(details)}",
                flush=True,
            )
        else:
            batch_id = timestamp_id()
            results = upload_full_snapshot(
                list(listing.values()), list(details.values()), batch_id=batch_id
            )
            uploaded = True
            for result in results:
                print(f"VICOFA_MINIO_OBJECT: {result['raw_object_key']}", flush=True)
    else:
        print("VICOFA_NO_NEW_ARTICLES: no MinIO object written", flush=True)

    return {
        "pages_scanned": pages_scanned,
        "known_articles": len(listing),
        "new_articles": discovered,
        "new_detail_records": new_detail_count,
        "uploaded_snapshots": int(uploaded),
        "used_existing_checkpoint": int(bool(baseline_count)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incremental VICOFA crawler -> immutable MinIO Bronze snapshots"
    )
    parser.add_argument("--max-pages", type=int, default=500)
    parser.add_argument(
        "--stop-after-known-pages",
        type=int,
        default=3,
        help="Stop incremental scan after this many consecutive pages contain no new IDs",
    )
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=4.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-stale",
        action="store_true",
        help="If the website denies access, continue only when a valid paired MinIO snapshot exists",
    )
    args = parser.parse_args()
    if args.max_pages < 1:
        parser.error("--max-pages must be >= 1")
    if args.stop_after_known_pages < 1:
        parser.error("--stop-after-known-pages must be >= 1")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Require 0 <= --delay-min <= --delay-max")

    run = start_run(DOMAIN, SOURCE)
    metrics: dict[str, int] = {
        "pages_scanned": 0,
        "known_articles": 0,
        "new_articles": 0,
        "new_detail_records": 0,
        "uploaded_snapshots": 0,
        "used_existing_checkpoint": 0,
        "stale_fallback": 0,
    }

    try:
        try:
            result = crawl(args)
            metrics.update({key: int(value) for key, value in result.items()})
        except SourceUnavailable as exc:
            if not args.allow_stale:
                raise
            client = get_minio_client()
            bucket = get_bucket_name()
            existing_list, existing_details = load_existing(client, bucket)
            if not existing_list or set(existing_list) != set(existing_details):
                raise RuntimeError(
                    "VICOFA site is unavailable and no complete existing MinIO checkpoint exists"
                ) from exc
            metrics.update(
                {
                    "known_articles": len(existing_list),
                    "used_existing_checkpoint": 1,
                    "stale_fallback": 1,
                }
            )
            print(
                "VICOFA_CRAWL_SKIPPED_STALE_BASELINE: "
                f"source unavailable ({exc}); reusing {len(existing_list)} paired MinIO articles",
                flush=True,
            )

        if args.dry_run:
            finish_run(run, dry_run=True, metrics=metrics)
        else:
            # A no-change run is still a successful, auditable checkpoint check.
            finish_run(run, result={"batch_id": None}, metrics=metrics)
    except Exception as exc:
        finish_run(run, error=exc, metrics=metrics)
        report_path = save_run(run)
        print(f"FAILED_RUN_REPORT: {report_path}", flush=True)
        raise

    report_path = save_run(run)
    print(f"VICOFA_RUN_REPORT: {report_path}", flush=True)
    print(
        "VICOFA_SUMMARY: "
        + json.dumps(metrics, ensure_ascii=False, sort_keys=True),
        flush=True,
    )


if __name__ == "__main__":
    main()
