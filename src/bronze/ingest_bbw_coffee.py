"""BBW coffee spot prices -> MinIO Bronze with checkpoint-first ingestion.

Run from the project root:
    python -m src.bronze.ingest_bbw_coffee --dry-run
    python -m src.bronze.ingest_bbw_coffee
    python -m src.bronze.ingest_bbw_coffee --refresh-days 0

Existing v1 monthly checkpoints in MinIO are reused. Missing historical ranges
are backfilled once; the most recent N days are refreshed as daily windows.
"""
from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.bronze.bbw_incremental import run_incremental
from src.bronze.metadata import upload_bytes_and_metadata
from src.bronze.run_logging import start_run, finish_run, save_run
from src.bronze.minio_client import get_bucket_name, get_minio_client

API_URL = "https://bbw.vn/api/proxy/commodity-spot-prices"
DOMAIN = "coffee_prices"
SOURCE = "bbw_coffee"
PIPELINE = "ingest_bbw_coffee_v1"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date YYYY-MM-DD: {value}") from exc


def http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "CoffeeLakehouseResearch/1.0",
        "Accept": "application/json",
    })
    return session


def request_page(
    session: requests.Session,
    start: date,
    end: date,
    page: int,
    limit: int,
):
    params = {
        "product": "COFFEE",
        "from": start.isoformat(),
        "to": end.isoformat(),
        "limit": limit,
        "page": page,
        "sort": "newest",
    }
    response = session.get(API_URL, params=params, timeout=(15, 90))
    if response.status_code >= 400:
        raise RuntimeError(
            f"BBW COFFEE HTTP {response.status_code}: "
            f"{response.text[:500]} | URL={response.url}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"BBW COFFEE returned non-JSON: {response.text[:250]}"
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise RuntimeError("Unexpected BBW COFFEE schema: missing data[]")
    api_meta = payload.get("meta") or {}
    if not isinstance(api_meta, dict):
        raise RuntimeError("Unexpected BBW COFFEE schema: meta is not an object")
    current_page = api_meta.get("current_page")
    if current_page is not None and int(current_page) != page:
        raise RuntimeError(
            f"BBW COFFEE page mismatch: requested={page}, returned={current_page}"
        )
    window = api_meta.get("window") or {}
    if window and (
        window.get("from") != start.isoformat()
        or window.get("to") != end.isoformat()
    ):
        raise RuntimeError(f"BBW COFFEE window mismatch: {window}")

    for record in payload["data"]:
        if not isinstance(record, dict):
            raise RuntimeError("Unexpected BBW COFFEE record type")
        record_date = record.get("quote_date_vn")
        try:
            parsed_date = date.fromisoformat(str(record_date)[:10])
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Invalid BBW COFFEE quote_date_vn: {record_date}"
            ) from exc
        if not start <= parsed_date <= end:
            raise RuntimeError(
                f"Out-of-window COFFEE quote_date_vn: {record_date}"
            )
        if record.get("product") != "COFFEE":
            raise RuntimeError(
                f"Unexpected BBW COFFEE product: {record.get('product')}"
            )

    return response.content, payload, response.url


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint-first incremental BBW COFFEE ingestion"
    )
    parser.add_argument("--start-date", type=parse_date, default=date(2015, 1, 1))
    parser.add_argument("--end-date", type=parse_date, default=None)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=3,
        help="Refresh only the last N days as daily windows; 0 disables refresh",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.3)
    args = parser.parse_args()

    today = datetime.now(VN_TZ).date()
    end = args.end_date or today
    if args.start_date > end:
        parser.error("--start-date must be <= --end-date")
    if end > today:
        parser.error("--end-date cannot be in the future")
    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")
    if not 0 <= args.refresh_days <= 30:
        parser.error("--refresh-days must be between 0 and 30")
    if args.sleep < 0:
        parser.error("--sleep must be nonnegative")

    run_id = datetime.now(VN_TZ).strftime("%Y%m%dt%H%M%S") + "_" + uuid.uuid4().hex[:8]
    print(
        f"BBW COFFEE: requested={args.start_date}..{end}, "
        f"refresh_days={args.refresh_days}, limit={args.limit}, run={run_id}"
    )
    print(
        "MODE: checkpoint-first; completed historical windows are not queried. "
        "Recent refreshes use one-day API windows."
    )
    print("NOTE: today's quote may be provisional; Bronze stores raw snapshots.")

    run = start_run(DOMAIN, SOURCE)
    session = None
    try:
        client = get_minio_client()
        bucket = get_bucket_name()
        if not client.bucket_exists(bucket):
            raise RuntimeError(f"MinIO bucket not found: {bucket}")
        session = http_session()

        def upload_page(
            raw, payload, url, window_start, window_end,
            page, last_page, is_refresh, limit,
        ):
            meta = payload.get("meta") or {}
            filename = (
                f"bbw_coffee_{window_start}_{window_end}_page_{page:04d}.json"
            )
            upload_bytes_and_metadata(
                raw_bytes=raw,
                raw_filename=filename,
                domain=DOMAIN,
                source=SOURCE,
                source_type="api",
                source_url=url,
                content_type="application/json",
                extra_metadata={
                    "pipeline": PIPELINE,
                    "run_batch_id": run_id,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "window_mode": "daily_refresh" if is_refresh else "historical",
                    "page": page,
                    "last_page": last_page,
                    "limit": limit,
                    "records": len(payload["data"]),
                    "total": meta.get("total"),
                    "snapshot_mode": meta.get("snapshot_mode"),
                    "is_refresh": is_refresh,
                    "is_current_day_window": window_start <= today <= window_end,
                    "note": "Quotes for current day may change after collection",
                },
            )
            print(
                f"  SUCCESS page {page}/{last_page}: "
                f"{len(payload['data'])} records",
                flush=True,
            )

        stats = run_incremental(
            client=client,
            bucket=bucket,
            domain=DOMAIN,
            source=SOURCE,
            pipeline=PIPELINE,
            start=args.start_date,
            end=end,
            refresh_days=args.refresh_days,
            limit=args.limit,
            dry_run=args.dry_run,
            request_page=lambda start, stop, page, limit: request_page(
                session, start, stop, page, limit
            ),
            upload_page=upload_page,
            sleep_seconds=args.sleep,
        )
        print(
            "SUMMARY: "
            f"api_calls={stats['api_calls']}, "
            f"uploaded_pages={stats['uploaded_pages']}, "
            f"skipped_pages={stats['skipped_pages']}, "
            f"complete_windows_skipped={stats['complete_windows_skipped']}, "
            f"planned_windows={stats['planned_windows']}, "
            f"refreshed_days={stats['refreshed_days']}",
            flush=True,
        )
    except Exception as exc:
        metrics = getattr(exc, "ingestion_metrics", locals().get("stats", {}))
        finish_run(
            run,
            error=exc,
            metrics={
                "api_calls": int(metrics.get("api_calls", 0)),
                "uploaded_pages": int(metrics.get("uploaded_pages", 0)),
                "skipped_pages": int(metrics.get("skipped_pages", 0)),
                "complete_windows_skipped": int(metrics.get("complete_windows_skipped", 0)),
                "planned_windows": int(metrics.get("planned_windows", 0)),
                "refreshed_days": int(metrics.get("refreshed_days", 0)),
            },
        )
        report_path = save_run(run)
        print(f"FAILED_RUN_REPORT: {report_path}", flush=True)
        raise
    else:
        metrics = stats
        if args.dry_run:
            finish_run(
                run,
                dry_run=True,
                metrics={
                    "api_calls": 0,
                    "uploaded_pages": 0,
                    "skipped_pages": int(metrics["skipped_pages"]),
                    "complete_windows_skipped": int(metrics["complete_windows_skipped"]),
                    "planned_windows": int(metrics["planned_windows"]),
                    "refreshed_days": int(metrics["refreshed_days"]),
                },
            )
        else:
            finish_run(
                run,
                result={"batch_id": run_id},
                metrics={
                    "api_calls": int(metrics["api_calls"]),
                    "uploaded_pages": int(metrics["uploaded_pages"]),
                    "skipped_pages": int(metrics["skipped_pages"]),
                    "complete_windows_skipped": int(metrics["complete_windows_skipped"]),
                    "planned_windows": int(metrics["planned_windows"]),
                    "refreshed_days": int(metrics["refreshed_days"]),
                },
            )
        report_path = save_run(run)
        print(f"RUN_REPORT: {report_path}", flush=True)
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    main()
