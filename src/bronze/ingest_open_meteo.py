"""Incremental Open-Meteo archive -> MinIO Bronze. Run from project root with -m."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from minio.error import S3Error

from src.bronze.metadata import upload_bytes_and_metadata
from src.bronze.minio_client import get_bucket_name, get_minio_client
from src.bronze.run_logging import finish_run, save_run, start_run

API = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = (
    "weather_code,temperature_2m_mean,temperature_2m_min,temperature_2m_max,"
    "rain_sum,precipitation_sum,et0_fao_evapotranspiration"
)
STATIONS = {
    "dak_nong": (12.4431, 107.9654, "open-meteo-12.48N107.97E419m_DakNong.csv"),
    "dak_lak": (12.83, 108.13, "open-meteo-12.83N108.13E481m_DaKLak.csv"),
    "gia_lai": (13.9833, 108.0, None),
    "lam_dong": (11.9465, 108.4419, None),
}
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Gia Lai and Lam Dong use corrected reference locations; old CSVs must not be
# joined to these corrected coordinate series.


def last_csv_day(path: Path) -> date:
    if not path.exists():
        raise FileNotFoundError(f"Historical CSV not found: {path}")
    latest = None
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as stream:
        for line in stream:
            first = line.split(",", 1)[0].strip().strip('"')
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", first):
                continue
            try:
                day = date.fromisoformat(first)
            except ValueError:
                continue
            if latest is None or day > latest:
                latest = day
    if latest is None:
        raise ValueError(f"No YYYY-MM-DD dates found in first CSV column: {path}")
    return latest


def verified_existing(client, bucket: str, station: str) -> set[date]:
    """Return only dates backed by a successful metadata record and existing raw object."""
    days: set[date] = set()
    suffix = "_corrected_v2" if station in ("gia_lai", "lam_dong") else ""
    prefix = f"bronze/weather/source=open_meteo_archive_{station}{suffix}/"

    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith("/metadata.json"):
            continue
        response = None
        try:
            response = client.get_object(bucket, obj.object_name)
            metadata = json.loads(response.read())
            if metadata.get("status") != "success":
                continue
            info = metadata.get("source_metadata") or {}
            date_from = info.get("date_from")
            date_to = info.get("date_to")
            raw_key = metadata.get("raw_object_key")
            if not date_from or not date_to or not raw_key or not raw_key.startswith(prefix):
                continue
            raw_stat = client.stat_object(bucket, raw_key)
            if raw_stat.size != metadata.get("size_bytes"):
                continue
            first = date.fromisoformat(date_from)
            last = date.fromisoformat(date_to)
            if last < first:
                continue
            for offset in range((last - first).days + 1):
                days.add(first + timedelta(days=offset))
        except (ValueError, KeyError, TypeError) as exc:
            print(f"WARN_INVALID_CHECKPOINT: {obj.object_name}: {exc}", flush=True)
        except S3Error as exc:
            # A missing/corrupt S3 object is not trusted. Network/connection
            # errors are deliberately allowed to fail the run so they cannot
            # turn a temporary MinIO outage into a full historical re-crawl.
            print(f"WARN_UNVERIFIED_CHECKPOINT: {obj.object_name}: {exc}", flush=True)
        finally:
            if response is not None:
                response.close()
                response.release_conn()
    return days


def contiguous_ranges(days: list[date]) -> list[tuple[date, date]]:
    ordered = sorted(set(days))
    if not ordered:
        return []
    output: list[tuple[date, date]] = []
    first = previous = ordered[0]
    for day in ordered[1:]:
        if day != previous + timedelta(days=1):
            output.append((first, previous))
            first = day
        previous = day
    output.append((first, previous))
    return output


def request_with_retry(
    url: str, *, params: dict, timeout: int = 90, attempts: int = 4, raise_for_status: bool = True
):
    response = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code in RETRYABLE_STATUS:
                if attempt == attempts:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(max(float(retry_after), 1.0), 60.0) if retry_after else min(2 ** attempt, 20)
                except ValueError:
                    delay = min(2 ** attempt, 20)
                print(f"RETRY {attempt}/{attempts}: HTTP {response.status_code}; waiting {delay:.1f}s", flush=True)
                time.sleep(delay)
                continue
            if raise_for_status:
                response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt == attempts:
                raise
            delay = min(2 ** attempt, 20)
            print(f"RETRY {attempt}/{attempts}: {exc}; waiting {delay}s", flush=True)
            time.sleep(delay)
    raise RuntimeError("Open-Meteo request exhausted retries")


def ingest_station(args, station: str) -> dict[str, int]:
    latitude, longitude, filename = STATIONS[station]
    if args.start_date:
        start = date.fromisoformat(args.start_date)
    elif filename:
        start = last_csv_day(Path(args.data_dir) / filename) + timedelta(days=1)
    else:
        start = date(2000, 1, 1)

    end = date.fromisoformat(args.end_date)
    if end < start:
        print(f"NO_NEW_DAYS: historical last day >= requested end ({end})", flush=True)
        return {"fetch_days": 0, "refresh_days": 0, "uploaded_chunks": 0}

    client = get_minio_client()
    bucket = get_bucket_name()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f"MinIO bucket not found: {bucket}")

    existing = verified_existing(client, bucket, station)
    refresh_start = (
        end - timedelta(days=args.refresh_days - 1)
        if args.refresh_days
        else end + timedelta(days=1)
    )
    requested_days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    missing = [day for day in requested_days if day not in existing or day >= refresh_start]
    refresh_count = sum(day in existing for day in missing)

    jobs: list[tuple[date, date]] = []
    for range_start, range_end in contiguous_ranges(missing):
        cursor = range_start
        while cursor <= range_end:
            last_chunk = min(cursor + timedelta(days=args.chunk_days - 1), range_end)
            jobs.append((cursor, last_chunk))
            cursor = last_chunk + timedelta(days=1)

    print(
        f"Station={station}, coords=({latitude}, {longitude}), "
        f"requested={start}..{end}, fetch_days={len(missing)}, refresh_days={refresh_count}",
        flush=True,
    )
    uploaded_chunks = 0
    for first, last in jobs:
        print(f"{'WOULD_FETCH' if args.dry_run else 'FETCH'}: {first}..{last}", flush=True)
        if args.dry_run:
            continue

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": first.isoformat(),
            "end_date": last.isoformat(),
            "daily": VARIABLES,
            "timezone": "Asia/Ho_Chi_Minh",
        }
        response = request_with_retry(API, params=params)
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(f"Open-Meteo error: {payload}")

        daily = payload.get("daily") or {}
        received = daily.get("time", [])
        expected = [
            (first + timedelta(days=offset)).isoformat()
            for offset in range((last - first).days + 1)
        ]
        if received != expected:
            raise RuntimeError(
                f"Incomplete/unexpected dates: expected {len(expected)}, got {len(received)}. Not uploading."
            )
        for field in VARIABLES.split(","):
            values = daily.get(field)
            if not isinstance(values, list) or len(values) != len(expected):
                raise RuntimeError(f"Missing/invalid daily field {field}; not uploading.")

        source = f"open_meteo_archive_{station}{'_corrected_v2' if filename is None else ''}"
        raw_name = f"{station}_{first}_{last}.json"
        result = upload_bytes_and_metadata(
            raw_bytes=response.content,
            raw_filename=raw_name,
            domain="weather",
            source=source,
            source_type="api",
            source_url=response.url,
            content_type="application/json",
            extra_metadata={
                "station": station,
                "latitude": latitude,
                "longitude": longitude,
                "date_from": str(first),
                "date_to": str(last),
                "timezone": "Asia/Ho_Chi_Minh",
                "record_count": len(received),
                "grid_latitude": payload.get("latitude"),
                "grid_longitude": payload.get("longitude"),
                "grid_elevation": payload.get("elevation"),
                "coordinate_version": "corrected_v2" if filename is None else "legacy_continuation_v1",
                "is_refresh": first <= end and last >= refresh_start,
                "contains_current_day": (
                    first <= datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date() <= last
                ),
            },
        )

        stored = client.get_object(bucket, result["raw_object_key"])
        try:
            digest = hashlib.sha256(stored.read()).hexdigest()
        finally:
            stored.close()
            stored.release_conn()
        if digest != result["sha256"]:
            raise RuntimeError("Read-back SHA256 mismatch")

        uploaded_chunks += 1
        print(f"SUCCESS: {len(received)} days; {result['raw_object_key']}", flush=True)
        time.sleep(1)

    if not jobs:
        print("SKIPPED: all requested days already ingested (outside refresh window)", flush=True)
    return {
        "fetch_days": len(missing),
        "refresh_days": refresh_count,
        "uploaded_chunks": uploaded_chunks,
    }


def resolve_default_end_date() -> date:
    """Use today only when Archive confirms it; otherwise use its reported cutoff."""
    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()
    params = {
        "latitude": STATIONS["dak_nong"][0],
        "longitude": STATIONS["dak_nong"][1],
        "start_date": (today - timedelta(days=7)).isoformat(),
        "end_date": today.isoformat(),
        "daily": VARIABLES,
        "timezone": "Asia/Ho_Chi_Minh",
    }
    response = request_with_retry(API, params=params, raise_for_status=False)
    if response.ok:
        payload = response.json()
        times = (payload.get("daily") or {}).get("time", [])
        if times and times[-1] == today.isoformat():
            print(f"ARCHIVE_END_DATE: {today} (today is available)", flush=True)
            return today
        raise RuntimeError("Archive probe returned unexpected dates; refusing to guess the cutoff")

    try:
        reason = response.json().get("reason", "")
    except ValueError:
        reason = response.text[:300]
    match = re.search(
        r"Parameter 'end_date' is out of allowed range from \d{4}-\d{2}-\d{2} to (\d{4}-\d{2}-\d{2})",
        reason,
    )
    if response.status_code == 400 and match:
        cutoff = date.fromisoformat(match.group(1))
        if cutoff > today:
            raise RuntimeError(f"Unexpected future Archive cutoff: {cutoff}")
        print(f"ARCHIVE_END_DATE: {cutoff} (today {today} not yet supported)", flush=True)
        return cutoff
    raise RuntimeError(f"Archive end-date probe failed: HTTP {response.status_code}: {reason}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incremental Open-Meteo Archive -> MinIO Bronze (4 stations)"
    )
    parser.add_argument("--station", required=True, choices=[*STATIONS, "all"])
    parser.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help=f"Directory containing historical weather CSVs (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument("--start-date", help="Override start date, YYYY-MM-DD (use with care)")
    parser.add_argument(
        "--end-date",
        help="Default: latest date accepted by Archive API, up to today in Asia/Ho_Chi_Minh",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chunk-days", type=int, default=365)
    parser.add_argument(
        "--refresh-days", type=int, default=3, help="Re-fetch the last N days; 0 disables refresh"
    )
    args = parser.parse_args()
    if not 1 <= args.chunk_days <= 365:
        parser.error("--chunk-days must be 1..365")
    if not 0 <= args.refresh_days <= 30:
        parser.error("--refresh-days must be 0..30")

    run = start_run("weather", "open_meteo_archive")
    stations = list(STATIONS) if args.station == "all" else [args.station]
    failed: list[str] = []
    totals = {"fetch_days": 0, "refresh_days": 0, "uploaded_chunks": 0}

    try:
        if args.end_date is None:
            args.end_date = resolve_default_end_date().isoformat()
        print(f"DATA_DIR: {Path(args.data_dir).resolve()}", flush=True)
        print(
            f"OPEN_METEO: stations={','.join(stations)}, end={args.end_date}, "
            f"refresh_days={args.refresh_days}, chunk_days={args.chunk_days}",
            flush=True,
        )
        for station in stations:
            print(f"\n===== {station.upper()} =====", flush=True)
            try:
                metrics = ingest_station(args, station)
                for key, value in metrics.items():
                    totals[key] += value
            except Exception as exc:
                print(f"ERROR [{station}]: {type(exc).__name__}: {exc}", flush=True)
                failed.append(station)
                if args.station != "all":
                    raise
        print(
            f"\nSUMMARY: stations={len(stations)}, succeeded={len(stations)-len(failed)}, "
            f"failed={len(failed)}, uploaded_chunks={totals['uploaded_chunks']}",
            flush=True,
        )
    except Exception as exc:
        finish_run(
            run,
            error=exc,
            metrics={
                "total_stations": len(stations),
                "succeeded_stations": len(stations) - len(failed),
                "failed_stations": len(failed),
                **totals,
            },
        )
        report_path = save_run(run)
        print(f"FAILED_RUN_REPORT: {report_path}", flush=True)
        raise

    metrics = {
        "total_stations": len(stations),
        "succeeded_stations": len(stations) - len(failed),
        "failed_stations": len(failed),
        **totals,
    }
    if failed:
        finish_run(run, error=RuntimeError("One or more Open-Meteo stations failed"), metrics=metrics)
    else:
        finish_run(
            run,
            dry_run=args.dry_run,
            result=None if args.dry_run else {"batch_id": run["run_id"]},
            metrics=metrics,
        )
    report_path = save_run(run)
    print(f"RUN_REPORT: {report_path}", flush=True)
    if failed:
        raise SystemExit(f"Failed stations: {', '.join(failed)}")


if __name__ == "__main__":
    main()
