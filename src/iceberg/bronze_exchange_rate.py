"""Ingest the five historical FX sources plus incremental GoogleFinance JSON."""

import hashlib
import io
from collections import Counter
from datetime import date, datetime
from decimal import Decimal

from openpyxl import load_workbook
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

from minio_direct import (
    MinioDirect, csv_records, googlefinance_sheet_records, json_record,
    record_id_for, spark_session, merge_into_bronze
)

TABLE = "lakehouse.bronze.exchange_rate_raw"
PREFIX = "bronze/exchange_rate/"

REQUIRED_HISTORICAL_SOURCES = {
    "usd_vnd_csv",
    "eur_vnd_csv",
    "usd_vnd_excel",
    "eur_vnd_excel",
    "imf_exchange_rate",
}
SUPPORTED_SOURCES = REQUIRED_HISTORICAL_SOURCES | {"googlefinance_sheet"}

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


def excel_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def excel_records(raw):
    workbook = load_workbook(
        io.BytesIO(raw),
        read_only=True,
        data_only=True
    )

    try:
        sheet = workbook["Sheet1"]

        # Row 1: title; Row 2: column headers
        headers = [
            str(value).strip() if value is not None else f"column_{i}"
            for i, value in enumerate(
                next(sheet.iter_rows(min_row=2, max_row=2, values_only=True)),
                start=1
            )
        ]

        if len(set(headers)) != len(headers):
            raise ValueError("Duplicate Excel column headers")

        for row_number, values in enumerate(
            sheet.iter_rows(min_row=3, values_only=True),
            start=3
        ):
            if not any(value is not None for value in values):
                continue

            record = {
                headers[i]: excel_value(value)
                for i, value in enumerate(values)
            }

            yield row_number, record
    finally:
        workbook.close()


def main():
    minio = MinioDirect()

    # Only raw data objects are parsed; metadata.json is retained in MinIO
    # for lineage but is not an exchange-rate observation file.
    keys = sorted(
        key for key in minio.list_keys(PREFIX)
        if "/raw/" in key.lower()
        and key.lower().endswith((".csv", ".xlsx", ".json"))
    )

    sources = {
        part.split("=", 1)[1]
        for key in keys
        for part in key.split("/")
        if part.startswith("source=")
    }

    missing = REQUIRED_HISTORICAL_SOURCES - sources
    unsupported = sources - SUPPORTED_SOURCES
    if not keys or missing or unsupported:
        raise RuntimeError(
            "Invalid FX source inventory: "
            f"missing={sorted(missing)}, unsupported={sorted(unsupported)}, "
            f"objects={len(keys)}"
        )

    rows = []
    counts = Counter()

    for key in keys:
        raw = minio.read(key)
        digest = hashlib.sha256(raw).hexdigest()

        source = next(
            part.split("=", 1)[1]
            for part in key.split("/")
            if part.startswith("source=")
        )

        if key.lower().endswith(".xlsx"):
            record_type = "excel"
            iterator = excel_records(raw)
        elif key.lower().endswith(".json"):
            if source != "googlefinance_sheet":
                raise ValueError(f"JSON is only supported for googlefinance_sheet: {key}")
            record_type = "json"
            iterator = enumerate(googlefinance_sheet_records(key, raw), start=1)
        else:
            record_type = "csv"
            iterator = enumerate(csv_records(key, raw), start=1)

        before = len(rows)

        for position, record in iterator:
            if not isinstance(record, dict):
                raise ValueError(f"Invalid record in {key}")

            rows.append((
                record_id_for(key, position),
                key,
                position,
                "exchange_rate",
                source,
                record_type,
                digest,
                json_record(record),
            ))

            counts[source] += 1

        if len(rows) == before:
            if source == "googlefinance_sheet":
                print(f"GOOGLEFINANCE_EMPTY_OBJECT_SKIPPED: {key}", flush=True)
                continue
            raise RuntimeError(f"Empty source file: {key}")

    print("SOURCE_OBJECTS:", len(keys), flush=True)
    print("RECORDS_BY_SOURCE:", dict(counts), flush=True)
    if counts["googlefinance_sheet"]:
        print("GOOGLEFINANCE_BRONZE_ROWS:", counts["googlefinance_sheet"], flush=True)

    spark = spark_session("TLCN-Bronze-Exchange-Rate")

    try:
        merge_into_bronze(
            spark,
            TABLE,
            SCHEMA,
            rows,
            "exchange_rate_direct",
            "BRONZE_EXCHANGE_RATE"
        )

        spark.sql(f"""
            SELECT source, record_type,
                   COUNT(*) AS records,
                   COUNT(DISTINCT object_key) AS source_objects
            FROM {TABLE}
            GROUP BY source, record_type
            ORDER BY source
        """).show(truncate=False)

        print("BRONZE_EXCHANGE_RATE_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
