"""BBW fuel prices -> MinIO Bronze (raw API JSON, paginated).

Place at src/bronze/ingest_bbw_fuel.py, run from project root:
    python -m src.bronze.ingest_bbw_fuel --dry-run
    python -m src.bronze.ingest_bbw_fuel
    python -m src.bronze.ingest_bbw_fuel --start-date 2025-04-24

Requires src.bronze.metadata.upload_bytes_and_metadata and
src.bronze.minio_client.get_minio_client/get_bucket_name.
"""
from __future__ import annotations

import argparse
import calendar
import json
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.bronze.metadata import upload_bytes_and_metadata
from src.bronze.minio_client import get_bucket_name, get_minio_client

API_URL = "https://bbw.vn/api/proxy/commodity-spot-prices"
DOMAIN = "fuel_prices"
SOURCE = "bbw_fuel"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date YYYY-MM-DD: {value}") from exc


def months_between(start: date, end: date):
    cursor = start
    while cursor <= end:
        last = date(cursor.year, cursor.month, calendar.monthrange(cursor.year, cursor.month)[1])
        window_end = min(last, end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, status=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "CoffeeLakehouseResearch/1.0", "Accept": "application/json"})
    return session


def existing_pages(client, bucket: str) -> set[tuple[str, str, int]]:
    """Recover successful page checkpoints from metadata (not raw object names)."""
    prefix = f"bronze/{DOMAIN}/source={SOURCE}/"
    done = set()
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith("/metadata.json"):
            continue
        response = None
        try:
            response = client.get_object(bucket, obj.object_name)
            meta = json.loads(response.read())
            extra = meta.get("source_metadata") or meta.get("extra_metadata") or {}
            # Backward compatibility with helpers that flatten extra keys.
            if not extra:
                extra = meta
            if extra.get("pipeline") != "ingest_bbw_fuel_v1":
                continue
            start, end, page = extra.get("window_start"), extra.get("window_end"), extra.get("page")
            if start and end and page is not None:
                done.add((str(start), str(end), int(page)))
        except Exception as exc:
            print(f"WARNING: cannot read checkpoint {obj.object_name}: {exc}", file=sys.stderr)
        finally:
            if response is not None:
                response.close()
                response.release_conn()
    return done


def request_page(session: requests.Session, start: date, end: date, page: int, limit: int):
    params = {
        "product": "FUEL", "from": start.isoformat(), "to": end.isoformat(),
        "limit": limit, "page": page, "sort": "newest", "snapshot_mode": "date_range",
    }
    resp = session.get(API_URL, params=params, timeout=(15, 90))
    if resp.status_code >= 400:
        raise RuntimeError(f"BBW HTTP {resp.status_code}: {resp.text[:500]} | URL={resp.url}")
    try:
        payload = resp.json()
    except ValueError as exc:
        raise RuntimeError(f"BBW returned non-JSON: {resp.text[:250]}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise RuntimeError("Unexpected BBW schema: missing data[]")
    api_meta = payload.get("meta") or {}
    if not isinstance(api_meta, dict):
        raise RuntimeError("Unexpected BBW schema: meta is not an object")
    current_page = api_meta.get("current_page")
    if current_page is not None and int(current_page) != page:
        raise RuntimeError(f"Page mismatch: requested={page}, returned={current_page}")
    window = api_meta.get("window") or {}
    if window and (window.get("from") != start.isoformat() or window.get("to") != end.isoformat()):
        raise RuntimeError(f"Window mismatch: {window}")
    for record in payload["data"]:
        if not isinstance(record, dict):
            raise RuntimeError("Unexpected BBW record type")
        record_date = record.get("quote_date_vn")
        if not record_date or not start.isoformat() <= record_date <= end.isoformat():
            raise RuntimeError(f"Out-of-window quote_date_vn: {record_date}")
        if record.get("product") != "FUEL":
            raise RuntimeError(f"Unexpected product: {record.get('product')}")
        if record.get("unit") not in ("VND/L", "VND/LITER", "VND/LITRE", "VND/KG", None):
            raise RuntimeError(f"Unexpected fuel unit: {record.get('unit')}")
    # Save raw bytes as received (do not json.dumps payload).
    return resp.content, payload, resp.url


def main():
    parser = argparse.ArgumentParser(description="Backfill and incrementally ingest BBW FUEL raw JSON")
    parser.add_argument("--start-date", type=parse_date, default=date(2025, 4, 24))
    parser.add_argument("--end-date", type=parse_date, default=None)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--refresh-days", type=int, default=3,
                        help="Always re-fetch the last N days (default 3)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.3, help="Delay between API requests")
    args = parser.parse_args()
    today = datetime.now(VN_TZ).date()
    end = args.end_date or today
    if args.start_date > end:
        parser.error("--start-date must be <= --end-date")
    if end > today:
        parser.error("--end-date cannot be in the future")
    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")
    if args.refresh_days < 0 or args.sleep < 0:
        parser.error("--refresh-days and --sleep must be nonnegative")

    run_id = datetime.now(VN_TZ).strftime("%Y%m%dt%H%M%S") + "_" + uuid.uuid4().hex[:8]
    refresh_start = (
        today - timedelta(days=args.refresh_days - 1)
        if args.refresh_days
        else today + timedelta(days=1)
    )
    print(f"BBW FUEL: {args.start_date}..{end}, refresh={args.refresh_days} days, run={run_id}")
    print("NOTE: fuel prices are event-based; Bronze stores original API snapshots.")

    client = get_minio_client()
    bucket = get_bucket_name()
    done = existing_pages(client, bucket)
    print(f"Existing page checkpoints: {len(done)}")
    session = http_session()
    saved = skipped = calls = 0
    try:
        for start, stop in months_between(args.start_date, end):
            # Use stable month-sized windows. Refresh the entire month when it
            # intersects the refresh interval; this avoids unstable split keys.
            parts = [(start, stop)]
            for wstart, wend in parts:
                is_refresh = wend >= refresh_start
                label = f"{wstart}..{wend}"
                print(f"WINDOW {label}{' [REFRESH]' if is_refresh else ''}")
                if args.dry_run:
                    print("  WOULD_CHECK pages via API; upload only missing pages unless refresh")
                    continue
                page = 1
                while True:
                    raw, payload, url = request_page(session, wstart, wend, page, args.limit)
                    calls += 1
                    m = payload.get("meta") or {}
                    last_page = int(m.get("last_page") or 1)
                    if last_page < 1 or page > last_page:
                        raise RuntimeError(f"Invalid pagination page={page}, last_page={last_page}")
                    key = (wstart.isoformat(), wend.isoformat(), page)
                    if key in done and not is_refresh:
                        skipped += 1
                        print(f"  SKIPPED page {page}/{last_page} (checkpoint exists)")
                    else:
                        # Every page is its own upload batch, preventing metadata.json collisions.
                        filename = f"bbw_fuel_{wstart}_{wend}_page_{page:04d}.json"
                        upload_bytes_and_metadata(
                            raw_bytes=raw, raw_filename=filename,
                            domain=DOMAIN, source=SOURCE, source_type="api",
                            source_url=url, content_type="application/json",
                            extra_metadata={
                                "pipeline": "ingest_bbw_fuel_v1", "run_batch_id": run_id,
                                "window_start": wstart.isoformat(), "window_end": wend.isoformat(),
                                "page": page, "last_page": last_page,
                                "records": len(payload["data"]),
                                "total": m.get("total"), "snapshot_mode": m.get("snapshot_mode"),
                                "is_refresh": is_refresh,
                                "is_current_day_window": wend == today,
                                "note": "Source is BBW API; provider is recorded in each raw record",
                            },
                        )
                        done.add(key)
                        saved += 1
                        print(f"  SUCCESS page {page}/{last_page}: {len(payload['data'])} records")
                    if page >= last_page:
                        break
                    page += 1
                    if args.sleep:
                        time.sleep(args.sleep)
                if args.sleep:
                    time.sleep(args.sleep)
    finally:
        session.close()
    print(f"SUMMARY: api_calls={calls}, uploaded_pages={saved}, skipped_pages={skipped}")


if __name__ == "__main__":
    main()
