"""Load London Futures CSV history and Giacaphe.com JSON snapshots from MinIO.

All objects under bronze/london_futures/ are scanned.
Supported raw formats:
  - CSV: historical seven-column London Futures files.
  - JSON: complete Giacaphe.com API snapshots; only coffee_liffe is used.

For each JSON snapshot, the nearest not-yet-expired delivery contract is selected
(e.g. RMF27 / 01/27 in the supplied 24-Nov-2026 preview) and mapped to the exact
seven historical CSV fields. The original JSON remains unchanged in MinIO.

Run:
    .\run_spark.ps1 -Script 'src/iceberg/bronze_london_futures.py'
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import PurePosixPath
from zoneinfo import ZoneInfo

from pyspark.sql.types import LongType, StringType, StructField, StructType

from minio_direct import (
    MinioDirect,
    csv_records,
    json_record,
    merge_into_bronze,
    record_id_for,
    spark_session,
)

PREFIX = "bronze/london_futures/"
TABLE = "lakehouse.bronze.london_futures_raw"
BATCH_ROWS = 2500
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

EXPECTED_COLUMNS = {
    "Ngày", "Lần cuối", "Mở", "Cao", "Thấp", "KL", "% Thay đổi",
}
QUOTE_GROUP = "coffee_liffe"
QUOTE_REQUIRED_FIELDS = {
    "Name", "Last", "PtcChange", "Volume", "High", "Low", "Open", "Time", "Month",
}
SYMBOL_PATTERN = re.compile(r"^RM[A-Z]\d{2}$")
MONTH_PATTERN = re.compile(r"^(0[1-9]|1[0-2])/(\d{2})$")

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("record_position", LongType(), False),
    StructField("domain", StringType(), False),
    StructField("source", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("object_sha256", StringType(), False),
    StructField("raw_record_json", StringType(), False),
])


def source_from_key(key: str) -> str:
    for part in key.split("/"):
        if part.startswith("source="):
            return part.split("=", 1)[1]
    return "legacy_unknown"


def list_data_objects(minio: MinioDirect) -> tuple[list[str], list[str]]:
    """Return all data objects and unsupported extensions; ignore metadata.json."""
    keys = sorted(
        key for key in minio.list_keys(PREFIX)
        if key.startswith(PREFIX)
        and not key.endswith("/")
        and PurePosixPath(key).name.lower() != "metadata.json"
    )
    supported = []
    unsupported = []
    for key in keys:
        suffix = PurePosixPath(key).suffix.lower()
        if suffix in {".csv", ".json"}:
            supported.append(key)
        else:
            unsupported.append(key)
    return supported, unsupported


def parse_decimal(value, *, field: str, key: str) -> Decimal:
    text = str(value).strip().replace(",", "")
    if not text:
        raise ValueError(f"Missing {field} in {key}")
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid {field}={value!r} in {key}") from exc
    if not number.is_finite():
        raise ValueError(f"Non-finite {field}={value!r} in {key}")
    return number


def parse_delivery_month(value: str, *, key: str, symbol: str) -> tuple[int, int]:
    match = MONTH_PATTERN.fullmatch(str(value).strip())
    if not match:
        raise ValueError(
            f"Invalid delivery Month={value!r} for {symbol} in {key}; expected MM/YY"
        )
    month = int(match.group(1))
    year = 2000 + int(match.group(2))
    return year, month


def validate_quote(quote: dict, *, key: str) -> tuple[str, tuple[int, int], datetime]:
    missing = QUOTE_REQUIRED_FIELDS - set(quote)
    if missing:
        raise ValueError(f"Missing quote fields in {key}: {sorted(missing)}")

    symbol = str(quote["Name"]).strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError(f"Not a London Robusta contract in {key}: {symbol!r}")

    delivery = parse_delivery_month(quote["Month"], key=key, symbol=symbol)
    for field in ("Last", "PtcChange", "Volume", "High", "Low", "Open"):
        parse_decimal(quote[field], field=field, key=key)

    timestamp = parse_decimal(quote["Time"], field="Time", key=key)
    if timestamp != timestamp.to_integral_value() or timestamp <= 0:
        raise ValueError(f"Invalid Unix timestamp for {symbol} in {key}: {timestamp}")

    quote_time = datetime.fromtimestamp(
        int(timestamp), tz=timezone.utc
    ).astimezone(VN_TZ)
    return symbol, delivery, quote_time


def select_front_contract(payload: dict, *, key: str) -> tuple[dict, str, datetime]:
    """Select the nearest delivery month on/after the quote's Vietnam calendar month."""
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {key}")

    quotes = payload.get(QUOTE_GROUP)
    if not isinstance(quotes, list) or not quotes:
        raise ValueError(f"Missing or empty {QUOTE_GROUP} list in {key}")

    candidates = []
    seen_symbols = set()
    for quote in quotes:
        if not isinstance(quote, dict):
            raise ValueError(f"Non-object quote in {key}")
        symbol, delivery, quote_time = validate_quote(quote, key=key)
        if symbol in seen_symbols:
            raise ValueError(f"Duplicate contract symbol {symbol} in {key}")
        seen_symbols.add(symbol)

        current_month = (quote_time.year, quote_time.month)
        if delivery >= current_month:
            candidates.append((delivery, symbol, quote, quote_time))

    if not candidates:
        raise ValueError(
            f"No active/future London Robusta delivery contract in {key}"
        )

    _, symbol, selected, quote_time = min(
        candidates, key=lambda item: (item[0], item[1])
    )
    return selected, symbol, quote_time


def normalized_api_record(payload: dict, *, key: str) -> tuple[dict, str]:
    """Map one selected front-month quote to the historical seven-column shape."""
    quote, symbol, quote_time = select_front_contract(payload, key=key)

    change = str(quote["PtcChange"]).strip()
    if not change.endswith("%"):
        change = f"{change}%"

    row = {
        "Ngày": quote_time.strftime("%d/%m/%Y"),
        "Lần cuối": str(quote["Last"]).strip(),
        "Mở": str(quote["Open"]).strip(),
        "Cao": str(quote["High"]).strip(),
        "Thấp": str(quote["Low"]).strip(),
        "KL": str(quote["Volume"]).strip(),
        "% Thay đổi": change,
    }
    return row, symbol


def validate_historical_csv_record(record: dict, *, key: str) -> dict:
    if set(record) != EXPECTED_COLUMNS:
        raise ValueError(
            f"Unexpected CSV columns in {key}: {sorted(record)}; "
            f"expected {sorted(EXPECTED_COLUMNS)}"
        )
    if not str(record.get("Ngày", "")).strip():
        raise ValueError(f"Historical CSV row has empty Ngày in {key}")
    return {column: str(record[column]).strip() for column in (
        "Ngày", "Lần cuối", "Mở", "Cao", "Thấp", "KL", "% Thay đổi"
    )}


def main():
    minio = MinioDirect()
    keys, unsupported = list_data_objects(minio)

    if unsupported:
        raise RuntimeError(
            "Unsupported object format(s) under "
            f"{PREFIX}: {unsupported}. Supported formats are CSV and JSON; "
            "metadata.json is excluded automatically."
        )
    if not keys:
        raise RuntimeError(f"No CSV/JSON data objects found under {PREFIX}")

    print(f"LONDON_SOURCE_OBJECTS: {len(keys)}", flush=True)
    print(
        "LONDON_SOURCE_FORMATS:",
        {suffix: sum(PurePosixPath(k).suffix.lower() == suffix for k in keys)
         for suffix in (".csv", ".json")},
        flush=True,
    )

    spark = spark_session("TLCN-Bronze-London-Futures-Direct")
    buffer: list[tuple] = []
    source_total = 0
    source_hashes: dict[str, str] = {}
    format_counts = {".csv": 0, ".json": 0}

    try:
        for object_index, key in enumerate(keys, start=1):
            raw = minio.read(key)
            digest = hashlib.sha256(raw).hexdigest()
            source_hashes[key] = digest
            source = source_from_key(key)
            suffix = PurePosixPath(key).suffix.lower()
            object_rows = 0

            if suffix == ".csv":
                for position, source_record in enumerate(
                    csv_records(key, raw), start=1
                ):
                    record = validate_historical_csv_record(
                        source_record, key=key
                    )
                    buffer.append((
                        record_id_for(key, position),
                        key,
                        position,
                        "london_futures",
                        source,
                        "london_futures_csv",
                        digest,
                        json_record(record),
                    ))
                    object_rows += 1

            elif suffix == ".json":
                try:
                    payload = json.loads(raw.decode("utf-8-sig"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError(f"Invalid JSON object: {key}") from exc

                record, selected_symbol = normalized_api_record(
                    payload, key=key
                )
                buffer.append((
                    record_id_for(key, 1),
                    key,
                    1,
                    "london_futures",
                    source,
                    "london_futures_api_front_contract",
                    digest,
                    json_record(record),
                ))
                object_rows = 1
                print(
                    f"LONDON_API_FRONT_CONTRACT: {selected_symbol} "
                    f"date={record['Ngày']} object={key}",
                    flush=True,
                )

            if object_rows == 0:
                raise RuntimeError(f"No data rows extracted from {key}")

            source_total += object_rows
            format_counts[suffix] += 1

            if len(buffer) >= BATCH_ROWS:
                merge_into_bronze(
                    spark, TABLE, SCHEMA, buffer,
                    "london_futures_incoming", "BRONZE_LONDON_BATCH",
                )
                buffer.clear()

            print(
                f"SOURCE_OBJECT: {object_index}/{len(keys)} "
                f"format={suffix} rows={object_rows} source={source}",
                flush=True,
            )

        if buffer:
            merge_into_bronze(
                spark, TABLE, SCHEMA, buffer,
                "london_futures_incoming", "BRONZE_LONDON_BATCH",
            )
            buffer.clear()

        total_iceberg = spark.table(TABLE).count()
        print(f"LONDON_CSV_OBJECTS: {format_counts['.csv']}", flush=True)
        print(f"LONDON_JSON_OBJECTS: {format_counts['.json']}", flush=True)
        print(f"LONDON_SOURCE_RECORDS: {source_total}", flush=True)
        print(f"LONDON_UNIQUE_CONTENTS: {len(set(source_hashes.values()))}", flush=True)
        print(f"BRONZE_LONDON_TOTAL: {total_iceberg}", flush=True)

        if total_iceberg != source_total:
            raise RuntimeError(
                f"Iceberg has {total_iceberg} records, MinIO yielded "
                f"{source_total}; investigate changed/removed source objects "
                "before continuing."
            )

        spark.sql(f"""
            SELECT source, record_type, count(*) AS records,
                   count(DISTINCT object_key) AS source_objects
            FROM {TABLE}
            GROUP BY source, record_type
            ORDER BY source, record_type
        """).show(truncate=False)

        print("BRONZE_LONDON_FUTURES_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
