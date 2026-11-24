"""Giacaphe.com London Robusta live quotes -> MinIO Bronze.

Run from the repository root:
    python -m src.bronze.ingest_giacaphe_london --dry-run
    python -m src.bronze.ingest_giacaphe_london

Stores the complete original JSON response as an immutable MinIO Bronze snapshot.
The London Futures Iceberg loader reads only payload["coffee_liffe"], selects the
nearest delivery contract, and maps it to the same seven fields as historical CSV.
The original API payload remains unchanged in MinIO.

No browser cookies, PHP session IDs, or Cloudflare clearance tokens are hardcoded.
"""
from __future__ import annotations

import argparse
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.bronze.metadata import upload_bytes_and_metadata
from src.bronze.minio_client import get_bucket_name, get_minio_client
from src.bronze.run_logging import finish_run, save_run, start_run


API_URL = (
    "https://giacaphe.com/live-quotes/"
    "quotes-update-nOsjt.php"
)
PAGE_URL = "https://giacaphe.com/gia-ca-phe-truc-tuyen/"

DOMAIN = "london_futures"
SOURCE = "giacaphe_london_robusta"
PIPELINE = "ingest_giacaphe_london_v2"

QUOTE_GROUP = "coffee_liffe"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Example source symbols: RMF27, RMH27, RMK27, RMN27, RMU27.
SYMBOL_PATTERN = re.compile(r"^RM[A-Z]\d{2}$")

REQUIRED_FIELDS = {
    "Name", "Last", "Change", "PtcChange", "Volume", "High", "Low",
    "Open", "Previous", "OpInt", "Bid", "BidSize", "Ask", "AskSize",
    "Time", "Month",
}

NUMERIC_FIELDS = (
    "Last", "Change", "PtcChange", "Volume", "High", "Low", "Open",
    "Previous", "OpInt", "Bid", "BidSize", "Ask", "AskSize", "Time",
)


def http_session(retries: int = 5) -> requests.Session:
    """Create a conservative HTTP session for the public JSON endpoint."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "CoffeeLakehouseResearch/1.0",
        "Accept": "application/json",
        "Referer": PAGE_URL,
        "X-Requested-With": "XMLHttpRequest",
        "X-Auth-Site": "giacaphe",
    })
    return session


def parse_numeric(value, *, field: str, symbol: str) -> Decimal:
    """Validate a numeric source value without changing the raw payload."""
    if value is None or str(value).strip() == "":
        raise ValueError(
            f"Missing numeric field {field!r} for contract {symbol}"
        )
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(
            f"Invalid numeric field {field!r}={value!r} for {symbol}"
        ) from exc
    if not number.is_finite():
        raise ValueError(
            f"Non-finite numeric field {field!r}={value!r} for {symbol}"
        )
    return number


def validate_london_quotes(payload: dict) -> list[dict]:
    """Validate only coffee_liffe; do not mix in coffee_ice or other groups."""
    if not isinstance(payload, dict):
        raise ValueError("API response root must be a JSON object")

    quotes = payload.get(QUOTE_GROUP)
    if not isinstance(quotes, list):
        raise ValueError(
            f"API response is missing the expected list {QUOTE_GROUP!r}"
        )
    if not quotes:
        raise ValueError(
            f"API returned an empty {QUOTE_GROUP!r} list; refusing to upload"
        )

    seen_symbols: set[str] = set()
    for index, quote in enumerate(quotes):
        if not isinstance(quote, dict):
            raise ValueError(f"{QUOTE_GROUP}[{index}] is not a JSON object")

        missing = REQUIRED_FIELDS - set(quote)
        if missing:
            raise ValueError(
                f"{QUOTE_GROUP}[{index}] is missing fields: {sorted(missing)}"
            )

        symbol = str(quote["Name"]).strip().upper()
        if not SYMBOL_PATTERN.fullmatch(symbol):
            raise ValueError(
                f"Unexpected London Robusta contract symbol at "
                f"{QUOTE_GROUP}[{index}]: {quote['Name']!r}"
            )
        if symbol in seen_symbols:
            raise ValueError(f"Duplicate contract symbol in response: {symbol}")
        seen_symbols.add(symbol)

        month = str(quote["Month"]).strip()
        if not re.fullmatch(r"(0[1-9]|1[0-2])/\d{2}", month):
            raise ValueError(
                f"Unexpected delivery month for {symbol}: {month!r}"
            )

        for field in NUMERIC_FIELDS:
            parse_numeric(quote[field], field=field, symbol=symbol)

        # Time is a Unix timestamp in seconds in the supplied response.
        timestamp = parse_numeric(quote["Time"], field="Time", symbol=symbol)
        if timestamp != timestamp.to_integral_value() or timestamp <= 0:
            raise ValueError(
                f"Invalid Unix timestamp for {symbol}: {timestamp}"
            )

    return quotes


def fetch_snapshot(
    session: requests.Session,
    *,
    timeout: tuple[int, int],
) -> tuple[bytes, dict, list[dict], str]:
    """Fetch and validate one live quote snapshot."""
    response = session.get(
        API_URL,
        params={"sid": "", "g": "coffee"},
        timeout=timeout,
    )

    if response.status_code >= 400:
        # Do not attempt to bypass Cloudflare or manufacture session cookies.
        raise RuntimeError(
            f"giacaphe.com HTTP {response.status_code}; "
            f"response={response.text[:500]!r}; URL={response.url}"
        )

    content_type = response.headers.get("Content-Type", "").lower()
    if "json" not in content_type:
        raise RuntimeError(
            "Expected application/json from giacaphe.com, "
            f"received Content-Type={content_type!r}; "
            f"body preview={response.text[:250]!r}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "giacaphe.com returned an invalid JSON response"
        ) from exc

    quotes = validate_london_quotes(payload)

    # response.content preserves the original JSON response body; do not
    # recreate the raw source file with json.dumps().
    return response.content, payload, quotes, response.url


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Capture giacaphe.com London Robusta live quotes "
            "and store the raw JSON snapshot in MinIO Bronze."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate the API response without uploading to MinIO.",
    )
    parser.add_argument(
        "--timeout-connect", type=int, default=15,
        help="HTTP connection timeout in seconds (default: 15).",
    )
    parser.add_argument(
        "--timeout-read", type=int, default=60,
        help="HTTP response timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--retries", type=int, default=5,
        help="Retries for connection errors and HTTP 429/5xx (default: 5).",
    )
    args = parser.parse_args()

    if args.timeout_connect <= 0 or args.timeout_read <= 0:
        parser.error("Timeout values must be positive.")
    if not 0 <= args.retries <= 10:
        parser.error("--retries must be between 0 and 10.")

    captured_utc = datetime.now(timezone.utc)
    captured_vn = captured_utc.astimezone(VN_TZ)
    run_batch_id = (
        captured_vn.strftime("%Y%m%dt%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    run = start_run(DOMAIN, SOURCE)
    session = None
    metrics = {
        "api_requests": 0,
        "quote_records": 0,
        "raw_objects_uploaded": 0,
    }

    try:
        session = http_session(retries=args.retries)
        raw_bytes, payload, quotes, source_url = fetch_snapshot(
            session,
            timeout=(args.timeout_connect, args.timeout_read),
        )

        metrics["api_requests"] = 1
        metrics["quote_records"] = len(quotes)
        symbols = sorted(str(row["Name"]).strip().upper() for row in quotes)

        print(f"SOURCE: {SOURCE}", flush=True)
        print(f"QUOTE_GROUP: {QUOTE_GROUP}", flush=True)
        print(f"API_URL: {source_url}", flush=True)
        print(f"SOURCE_UPDATED: {payload.get('updated')}", flush=True)
        print(f"CAPTURED_AT_UTC: {captured_utc.isoformat()}", flush=True)
        print(f"CAPTURED_AT_VN: {captured_vn.isoformat()}", flush=True)
        print(f"LONDON_ROBUSTA_CONTRACTS: {len(quotes)}", flush=True)
        print(f"CONTRACT_SYMBOLS: {','.join(symbols)}", flush=True)
        print(f"RAW_RESPONSE_BYTES: {len(raw_bytes)}", flush=True)

        if args.dry_run:
            print("DRY_RUN: API fetch and schema validation passed.", flush=True)
            print("NO_MINIO_UPLOAD", flush=True)
            finish_run(run, dry_run=True, metrics=metrics)
        else:
            client = get_minio_client()
            bucket = get_bucket_name()
            if not client.bucket_exists(bucket):
                raise RuntimeError(f"MinIO bucket not found: {bucket}")

            raw_filename = (
                "giacaphe_london_robusta_"
                f"{captured_vn.strftime('%Y%m%dT%H%M%S')}.json"
            )
            result = upload_bytes_and_metadata(
                raw_bytes=raw_bytes,
                raw_filename=raw_filename,
                domain=DOMAIN,
                source=SOURCE,
                source_type="api",
                source_url=source_url,
                content_type="application/json",
                extra_metadata={
                    "pipeline": PIPELINE,
                    "run_batch_id": run_batch_id,
                    "quote_group": QUOTE_GROUP,
                    "source_updated": payload.get("updated"),
                    "captured_at_utc": captured_utc.isoformat(),
                    "captured_at_vn": captured_vn.isoformat(),
                    "timezone": "Asia/Ho_Chi_Minh",
                    "quote_count": len(quotes),
                    "contract_symbols": symbols,
                    "snapshot_type": "live_quote_snapshot",
                    "raw_payload_scope": "complete_api_response",
                    "selected_data_scope": QUOTE_GROUP,
                    "price_unit": "not_assumed; verify source documentation",
                    "note": (
                        "MinIO preserves the complete original API response unchanged. "
                        "The London Futures Iceberg loader reads only coffee_liffe, "
                        "selects the nearest delivery contract, and maps it to the "
                        "seven historical CSV fields: Ngày, Lần cuối, Mở, Cao, "
                        "Thấp, KL, % Thay đổi. Other JSON groups are excluded."
                    ),
                },
            )

            metrics["raw_objects_uploaded"] = 1
            print(f"MINIO_BUCKET: {result['bucket']}", flush=True)
            print(f"RAW_OBJECT_KEY: {result['raw_object_key']}", flush=True)
            print(
                f"METADATA_OBJECT_KEY: {result['metadata_object_key']}",
                flush=True,
            )
            print(f"RAW_SHA256: {result['sha256']}", flush=True)
            print("GIACAPHE_LONDON_BRONZE_UPLOAD_PASSED", flush=True)
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
