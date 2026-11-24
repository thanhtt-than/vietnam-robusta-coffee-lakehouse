"""Incremental GoogleFinance FX API ingestion -> MinIO Bronze.

Run from the repository root:
    py -m src.bronze.ingest_googlefinance_fx --dry-run
    py -m src.bronze.ingest_googlefinance_fx

The complete HTTP response is stored unchanged as raw JSON with metadata.json.
The URL can be overridden with --url or GOOGLEFINANCE_FX_API_URL.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.bronze.metadata import upload_bytes_and_metadata
from src.bronze.run_logging import finish_run, save_run, start_run


# Do not hard-code a deployment URL: Apps Script deployment IDs can change.
# Pass --url or set GOOGLEFINANCE_FX_API_URL to the current Web App URL.
DEFAULT_URL = "https://script.google.com/macros/s/AKfycbwNG2-73SxaQXXndhUmUqrfQa61hgCPhgey1SNKtsr_DaACH1PYmsObYov6-BhuXYSuYA/exec"
DOMAIN = "exchange_rate"
SOURCE = "googlefinance_sheet"
DEFAULT_OUTPUT_NAME = "googlefinance_exchange_rate.json"


def make_session(retries: int) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "CoffeeLakehouseResearch/1.0",
        "Accept": "application/json",
    })
    return session


def _parse_observation_date(value: Any) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("Observation is missing date")

    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass

    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue

    raise ValueError(f"Unsupported observation date: {value!r}")


def _validate_pair_rows(pair_name: str, rows: Any) -> dict[str, Any]:
    if not isinstance(rows, list):
        raise ValueError(f"data.{pair_name} must be a JSON array")

    valid_dates = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(
                f"data.{pair_name}[{index - 1}] must be a JSON object"
            )

        raw_date = row.get("date", row.get("Date"))
        raw_close = row.get("close", row.get("Close"))

        if raw_date in (None, ""):
            raise ValueError(f"data.{pair_name}[{index - 1}] is missing date")
        if raw_close in (None, ""):
            raise ValueError(f"data.{pair_name}[{index - 1}] is missing close")

        observation_date = _parse_observation_date(raw_date)
        try:
            close = Decimal(str(raw_close).replace(",", "").strip())
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(
                f"Invalid close in data.{pair_name}[{index - 1}]: {raw_close!r}"
            ) from exc

        if not close.is_finite() or close <= 0:
            raise ValueError(
                f"Close must be a positive number in data.{pair_name}[{index - 1}]"
            )

        valid_dates.append(observation_date)

    if not valid_dates:
        raise ValueError(
            f"data.{pair_name} is EMPTY (0 observations). "
            "Refusing to upload an empty GoogleFinance snapshot to MinIO."
        )

    return {
        "count": len(valid_dates),
        "first_date": min(valid_dates).isoformat(),
        "last_date": max(valid_dates).isoformat(),
    }


def validate_response(raw: bytes) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if not raw:
        raise ValueError("GoogleFinance API returned an empty response body")

    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("GoogleFinance API did not return valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("GoogleFinance JSON root must be an object")
    if payload.get("error"):
        raise RuntimeError(f"GoogleFinance API error: {payload['error']}")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("GoogleFinance JSON is missing the data object")

    # Both pairs are required by this pipeline. An empty or missing pair must
    # stop ingestion before MinIO so a bad snapshot cannot be mistaken for success.
    summary = {
        "USD_VND": _validate_pair_rows("USD_VND", data.get("USD_VND")),
        "EUR_VND": _validate_pair_rows("EUR_VND", data.get("EUR_VND")),
    }
    return payload, summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch GoogleFinance USD/VND and EUR/VND data into MinIO Bronze."
    )
    parser.add_argument(
        "--url",
        default=os.getenv("GOOGLEFINANCE_FX_API_URL", DEFAULT_URL),
        help="Google Apps Script endpoint (default: GOOGLEFINANCE_FX_API_URL or project URL).",
    )
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--output-name", default=DEFAULT_OUTPUT_NAME)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.url.strip():
        parser.error(
            "Missing Apps Script Web App URL. Pass --url or set "
            "GOOGLEFINANCE_FX_API_URL to the current deployment URL."
        )
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if not 0 <= args.retries <= 10:
        parser.error("--retries must be between 0 and 10")
    if Path(args.output_name).name != args.output_name:
        parser.error("--output-name must be a filename, not a path")

    run = start_run(DOMAIN, SOURCE)
    session = None
    metrics = {
        "api_requests": 0,
        "response_bytes": 0,
        "raw_objects_uploaded": 0,
    }

    try:
        session = make_session(args.retries)
        response = session.get(args.url, timeout=args.timeout)
        response.raise_for_status()

        payload, pair_summary = validate_response(response.content)
        metrics["api_requests"] = 1
        metrics["response_bytes"] = len(response.content)
        metrics["usd_vnd_records"] = pair_summary["USD_VND"]["count"]
        metrics["eur_vnd_records"] = pair_summary["EUR_VND"]["count"]

        top_level_keys = sorted(payload.keys())
        print(f"API_HTTP_STATUS: {response.status_code}", flush=True)
        print(f"API_URL: {response.url}", flush=True)
        print(f"CONTENT_TYPE: {response.headers.get('Content-Type')}", flush=True)
        print(f"RAW_RESPONSE_BYTES: {len(response.content)}", flush=True)
        if top_level_keys:
            print(f"JSON_TOP_LEVEL_KEYS: {','.join(top_level_keys)}", flush=True)

        for pair_name, summary in pair_summary.items():
            print(f"{pair_name}_RECORDS: {summary['count']}", flush=True)
            print(
                f"{pair_name}_DATE_RANGE: "
                f"{summary['first_date']} -> {summary['last_date']}",
                flush=True,
            )

        if args.dry_run:
            print("GOOGLEFINANCE_FX_DRY_RUN_PASSED", flush=True)
            print("NO_MINIO_UPLOAD", flush=True)
            finish_run(run, dry_run=True, metrics=metrics)
        else:
            result = upload_bytes_and_metadata(
                raw_bytes=response.content,
                raw_filename=args.output_name,
                domain=DOMAIN,
                source=SOURCE,
                source_type="api",
                source_url=response.url,
                content_type="application/json",
                batch_id=args.batch_id,
                extra_metadata={
                    "pipeline": "ingest_googlefinance_fx",
                    "http_method": "GET",
                    "http_status": response.status_code,
                    "response_content_type": response.headers.get("Content-Type"),
                    "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                    "payload_top_level_keys": top_level_keys,
                    "pair_summary": pair_summary,
                    "raw_payload_scope": "complete_api_response",
                    "note": (
                        "The complete Google Apps Script JSON response is preserved unchanged. "
                        "Bronze Iceberg extracts USD_VND and EUR_VND observations."
                    ),
                },
            )
            metrics["raw_objects_uploaded"] = 1
            for key, value in result.items():
                print(f"{key}: {value}", flush=True)
            print("GOOGLEFINANCE_FX_BRONZE_UPLOAD_PASSED", flush=True)
            finish_run(run, result=result, metrics=metrics)

        report_path = save_run(run)
        print(f"RUN_REPORT: {report_path}", flush=True)

    except Exception as exc:
        if run.get("status") == "RUNNING":
            finish_run(run, error=exc, metrics=metrics)
            report_path = save_run(run)
            print(f"FAILED_RUN_REPORT: {report_path}", flush=True)
        raise
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    main()
