"""Shared checkpoint and incremental-window logic for BBW ingestion.

The helper reads the existing MinIO metadata layout produced by
src.bronze.metadata.upload_bytes_and_metadata. Existing v1 monthly checkpoints
remain usable; new missing ranges are fetched once, while recent days are
refreshed as independent one-day windows.
"""
from __future__ import annotations

import sys
import time
from datetime import date, timedelta
from typing import Any, Callable

PageRequest = Callable[[date, date, int, int], tuple[bytes, dict[str, Any], str]]
PageUpload = Callable[[bytes, dict[str, Any], str, date, date, int, int, bool, int], None]


def _read_metadata(client, bucket: str, object_name: str) -> dict[str, Any] | None:
    response = None
    try:
        response = client.get_object(bucket, object_name)
        import json
        metadata = json.loads(response.read())
        if not isinstance(metadata, dict):
            raise ValueError("metadata is not a JSON object")
        return metadata
    finally:
        if response is not None:
            response.close()
            response.release_conn()


def load_checkpoints(
    client,
    bucket: str,
    *,
    domain: str,
    source: str,
    pipeline: str,
    default_limit: int = 100,
) -> dict[tuple[date, date], dict[str, Any]]:
    """Load only successful checkpoints whose raw object still exists at expected size.

    Old metadata did not persist `limit`; those checkpoints are interpreted as
    limit=100, matching the original scripts' default.
    """
    import json  # noqa: F401 - documents that metadata is JSON

    prefix = f"bronze/{domain}/source={source}/"
    index: dict[tuple[date, date], dict[str, Any]] = {}
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith("/metadata.json"):
            continue
        try:
            metadata = _read_metadata(client, bucket, obj.object_name)
            if not metadata or metadata.get("status") != "success":
                continue
            extra = (
                metadata.get("source_metadata")
                or metadata.get("extra_metadata")
                or metadata
            )
            if not isinstance(extra, dict) or extra.get("pipeline") != pipeline:
                continue

            raw_key = metadata.get("raw_object_key")
            expected_size = metadata.get("size_bytes")
            if not raw_key or expected_size is None:
                continue
            stat = client.stat_object(bucket, raw_key)
            if int(stat.size) != int(expected_size):
                print(
                    f"WARNING: checkpoint raw size mismatch: {raw_key}",
                    file=sys.stderr,
                )
                continue

            start = date.fromisoformat(str(extra["window_start"]))
            end = date.fromisoformat(str(extra["window_end"]))
            page = int(extra["page"])
            last_page = int(extra.get("last_page") or 1)
            # Metadata written before this revision has no limit field.
            limit = int(extra.get("limit") or default_limit)
            if end < start or page < 1 or last_page < 1 or limit < 1:
                raise ValueError("invalid checkpoint window/page/limit")

            key = (start, end)
            window = index.setdefault(key, {"variants": {}})
            variant = window["variants"].setdefault(
                limit, {"pages": set(), "last_pages": set()}
            )
            variant["pages"].add(page)
            variant["last_pages"].add(last_page)
        except Exception as exc:
            print(
                f"WARNING: cannot index checkpoint {obj.object_name}: {exc}",
                file=sys.stderr,
            )
    return index


def _variant(index, start: date, end: date, limit: int):
    window = index.get((start, end))
    if not window:
        return None
    return window.get("variants", {}).get(limit)


def checkpoint_complete(
    index: dict[tuple[date, date], dict[str, Any]],
    start: date,
    end: date,
    limit: int,
) -> bool:
    variant = _variant(index, start, end, limit)
    if not variant or len(variant["last_pages"]) != 1:
        return False
    last_page = next(iter(variant["last_pages"]))
    return all(page in variant["pages"] for page in range(1, last_page + 1))


def _window_intersects(start: date, end: date, range_start: date, range_end: date) -> bool:
    return start <= range_end and end >= range_start


def _merge_intervals(
    intervals: list[tuple[date, date]],
) -> list[tuple[date, date]]:
    if not intervals:
        return []
    intervals = sorted(intervals)
    merged: list[list[date]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1] + timedelta(days=1):
            merged.append([start, end])
        elif end > merged[-1][1]:
            merged[-1][1] = end
    return [(item[0], item[1]) for item in merged]


def uncovered_ranges(
    start: date,
    end: date,
    covered: list[tuple[date, date]],
) -> list[tuple[date, date]]:
    """Return date intervals not covered by any trusted checkpoint window."""
    if end < start:
        return []
    cursor = start
    gaps: list[tuple[date, date]] = []
    for covered_start, covered_end in _merge_intervals(covered):
        if covered_end < cursor:
            continue
        if covered_start > end:
            break
        if covered_start > cursor:
            gaps.append((cursor, min(end, covered_start - timedelta(days=1))))
        cursor = max(cursor, covered_end + timedelta(days=1))
        if cursor > end:
            break
    if cursor <= end:
        gaps.append((cursor, end))
    return gaps


def split_by_calendar_month(
    start: date, end: date
) -> list[tuple[date, date]]:
    """Split a missing range into stable month-bounded API windows."""
    result = []
    cursor = start
    while cursor <= end:
        if cursor.month == 12:
            next_month = date(cursor.year + 1, 1, 1)
        else:
            next_month = date(cursor.year, cursor.month + 1, 1)
        window_end = min(end, next_month - timedelta(days=1))
        result.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return result


def build_plan(
    index: dict[tuple[date, date], dict[str, Any]],
    *,
    start: date,
    end: date,
    refresh_days: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    """Plan only incomplete historical windows, uncovered history, and recent refresh days."""
    refresh_start = (
        max(start, end - timedelta(days=refresh_days - 1))
        if refresh_days > 0
        else end + timedelta(days=1)
    )
    historical_end = min(end, refresh_start - timedelta(days=1))
    plan: list[dict[str, Any]] = []
    covered: list[tuple[date, date]] = []
    complete_windows = 0

    if historical_end >= start:
        existing = sorted(index.keys())
        for window_start, window_end in existing:
            if not _window_intersects(window_start, window_end, start, historical_end):
                continue
            covered.append((window_start, window_end))
            if checkpoint_complete(index, window_start, window_end, limit):
                complete_windows += 1
            else:
                plan.append({
                    "start": window_start,
                    "end": window_end,
                    "refresh": False,
                    "reason": "resume_incomplete_checkpoint",
                })

        for gap_start, gap_end in uncovered_ranges(start, historical_end, covered):
            for window_start, window_end in split_by_calendar_month(gap_start, gap_end):
                plan.append({
                    "start": window_start,
                    "end": window_end,
                    "refresh": False,
                    "reason": "missing_historical_range",
                })

    if refresh_days > 0:
        day = refresh_start
        while day <= end:
            plan.append({
                "start": day,
                "end": day,
                "refresh": True,
                "reason": "recent_daily_refresh",
            })
            day += timedelta(days=1)

    plan.sort(key=lambda item: (item["start"], item["end"], item["refresh"]))
    return plan, complete_windows


def _last_page(payload: dict[str, Any]) -> int:
    meta = payload.get("meta") or {}
    try:
        value = int(meta.get("last_page") or 1)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Invalid BBW last_page: {meta.get('last_page')}") from exc
    if value < 1:
        raise RuntimeError(f"Invalid BBW last_page: {value}")
    return value


def ingest_window(
    *,
    index: dict[tuple[date, date], dict[str, Any]],
    start: date,
    end: date,
    limit: int,
    is_refresh: bool,
    request_page: PageRequest,
    upload_page: PageUpload,
    sleep_seconds: float,
) -> dict[str, int]:
    """Fetch only missing pages; refresh windows deliberately fetch every page."""
    key = (start, end)
    variant = _variant(index, start, end, limit)
    calls = uploaded = skipped = 0

    if is_refresh:
        # Always create a fresh snapshot for recent daily windows.
        pages_to_fetch = None
    elif variant and len(variant["last_pages"]) == 1:
        last_page = next(iter(variant["last_pages"]))
        pages_to_fetch = [
            page for page in range(1, last_page + 1)
            if page not in variant["pages"]
        ]
        skipped = last_page - len(pages_to_fetch)
        if not pages_to_fetch:
            return {"api_calls": 0, "uploaded_pages": 0, "skipped_pages": skipped}
    else:
        # No reliable page count or conflicting old metadata: rebuild this window.
        pages_to_fetch = None

    def fetch_and_upload(page: int, expected_last_page: int | None):
        nonlocal calls, uploaded
        raw, payload, url = request_page(start, end, page, limit)
        calls += 1
        actual_last_page = _last_page(payload)
        if expected_last_page is not None and actual_last_page != expected_last_page:
            raise RuntimeError(
                f"BBW pagination changed during window {start}..{end}: "
                f"expected last_page={expected_last_page}, got {actual_last_page}. "
                "Re-run this window after reviewing the source response."
            )
        upload_page(
            raw, payload, url, start, end, page, actual_last_page,
            is_refresh, limit,
        )
        uploaded += 1
        window = index.setdefault(key, {"variants": {}})
        current = window["variants"].setdefault(
            limit, {"pages": set(), "last_pages": set()}
        )
        current["pages"].add(page)
        current["last_pages"] = {actual_last_page}
        return actual_last_page

    try:
        if pages_to_fetch is None:
            # Rebuild from page 1 so the page count and page set come from one run.
            page = 1
            last_page = fetch_and_upload(page, None)
            page += 1
            while page <= last_page:
                fetch_and_upload(page, last_page)
                page += 1
                if sleep_seconds:
                    time.sleep(sleep_seconds)
        else:
            expected_last = next(iter(variant["last_pages"]))
            for page in pages_to_fetch:
                fetch_and_upload(page, expected_last)
                if sleep_seconds:
                    time.sleep(sleep_seconds)
    except Exception as exc:
        setattr(exc, "ingestion_metrics", {
            "api_calls": calls,
            "uploaded_pages": uploaded,
            "skipped_pages": skipped,
        })
        raise

    return {
        "api_calls": calls,
        "uploaded_pages": uploaded,
        "skipped_pages": skipped,
    }


def run_incremental(
    *,
    client,
    bucket: str,
    domain: str,
    source: str,
    pipeline: str,
    start: date,
    end: date,
    refresh_days: int,
    limit: int,
    dry_run: bool,
    request_page: PageRequest,
    upload_page: PageUpload,
    sleep_seconds: float,
) -> dict[str, int]:
    index = load_checkpoints(
        client, bucket, domain=domain, source=source,
        pipeline=pipeline, default_limit=100,
    )
    plan, complete_windows = build_plan(
        index, start=start, end=end, refresh_days=refresh_days, limit=limit
    )
    stats = {
        "api_calls": 0,
        "uploaded_pages": 0,
        "skipped_pages": 0,
        "complete_windows_skipped": complete_windows,
        "planned_windows": len(plan),
        "refreshed_days": sum(1 for item in plan if item["refresh"]),
    }
    print(f"Verified checkpoint windows: {len(index)}")
    print(f"Complete historical windows skipped: {complete_windows}")

    if dry_run:
        for item in plan:
            start_label = item["start"].isoformat()
            end_label = item["end"].isoformat()
            action = "WOULD_REFRESH" if item["refresh"] else "WOULD_FETCH_MISSING"
            print(
                f"{action}: {start_label}..{end_label} "
                f"[{item['reason']}]"
            )
        if not plan:
            print("NO_WORK: all requested historical windows are checkpointed.")
        print("DRY_RUN: no BBW API requests and no MinIO uploads were made.")
        return stats

    for item in plan:
        window_start = item["start"]
        window_end = item["end"]
        label = f"{window_start}..{window_end}"
        print(
            f"WINDOW {label}"
            f"{' [DAILY REFRESH]' if item['refresh'] else ''}"
            f" [{item['reason']}]",
            flush=True,
        )
        try:
            result = ingest_window(
                index=index,
                start=window_start,
                end=window_end,
                limit=limit,
                is_refresh=item["refresh"],
                request_page=request_page,
                upload_page=upload_page,
                sleep_seconds=sleep_seconds,
            )
        except Exception as exc:
            partial = getattr(exc, "ingestion_metrics", {})
            for key in ("api_calls", "uploaded_pages", "skipped_pages"):
                stats[key] += int(partial.get(key, 0))
            setattr(exc, "ingestion_metrics", dict(stats))
            raise
        for key in ("api_calls", "uploaded_pages", "skipped_pages"):
            stats[key] += result[key]
        if result["api_calls"] == 0 and not item["refresh"]:
            print(f"  SKIPPED: window already complete ({result['skipped_pages']} pages)")
    return stats
