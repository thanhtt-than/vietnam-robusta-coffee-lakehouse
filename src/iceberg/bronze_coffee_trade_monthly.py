"""Ingest raw monthly coffee-export CSVs from MinIO into Bronze Iceberg."""

import csv
import hashlib
import io
import re
import unicodedata
from collections import Counter

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

from minio_direct import (
    MinioDirect,
    record_id_for,
    json_record,
    spark_session,
    merge_into_bronze,
)

TABLE = "lakehouse.bronze.coffee_trade_monthly_raw"
PREFIX = "bronze/trade/source=nguoi2_customs_csv/"

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("record_position", LongType(), False),
    StructField("domain", StringType(), False),
    StructField("source", StringType(), False),
    StructField("source_group", StringType(), False),
    StructField("source_year", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("object_sha256", StringType(), False),
    StructField("raw_record_json", StringType(), False),
])


def normalize_text(value):
    value = unicodedata.normalize("NFD", value.casefold())
    value = "".join(
        ch for ch in value
        if unicodedata.category(ch) != "Mn"
    )
    return value.replace("\u0111", "d")


def classify_source(filename):
    name = normalize_text(filename)

    if name.startswith("mot so mat hang xuat khau"):
        return "export_commodity_by_market"

    if name.startswith("tri gia va mat hang xuat khau"):
        return "export_commodity_total"

    if name.startswith("tri gia xuat khau phan theo"):
        return "export_value_by_market"

    raise ValueError("Unknown source group: " + filename)


def main():
    client = MinioDirect()

    keys = sorted(
        key for key in client.list_keys(PREFIX)
        if key.lower().endswith(".csv")
    )

    print("SOURCE_OBJECTS:", len(keys), flush=True)

    if len(keys) != 36:
        raise RuntimeError(
            f"Expected 36 source CSVs, found {len(keys)}"
        )

    rows = []
    by_group = Counter()
    by_year = Counter()

    for key in keys:
        filename = key.rsplit("/", 1)[-1]

        match = re.search(r"(20\d{2})\.csv$", filename)
        if not match:
            raise ValueError("Missing source year: " + filename)

        year = match.group(1)
        group = classify_source(filename)

        raw = client.read(key)
        digest = hashlib.sha256(raw).hexdigest()

        reader = csv.reader(
            io.StringIO(
                raw.decode("utf-8-sig"),
                newline=""
            )
        )

        file_rows = 0

        for position, cells in enumerate(reader, start=1):
            rows.append((
                record_id_for(key, position),
                key,
                position,
                "coffee_trade_monthly",
                "nguoi2_customs_csv",
                group,
                year,
                "csv",
                digest,
                json_record({"cells": cells}),
            ))

            file_rows += 1

        if file_rows == 0:
            raise RuntimeError("Empty CSV: " + key)

        by_group[group] += 1
        by_year[year] += 1

    expected_groups = {
        "export_commodity_by_market": 12,
        "export_commodity_total": 12,
        "export_value_by_market": 12,
    }

    expected_years = {
        str(year): 3 for year in range(2015, 2027)
    }

    if dict(by_group) != expected_groups:
        raise RuntimeError(
            f"Unexpected group distribution: {dict(by_group)}"
        )

    if dict(by_year) != expected_years:
        raise RuntimeError(
            f"Unexpected year distribution: {dict(by_year)}"
        )

    print("SOURCE_GROUPS:", dict(by_group), flush=True)
    print("SOURCE_YEARS:", dict(sorted(by_year.items())), flush=True)
    print("RAW_LOGICAL_ROWS:", len(rows), flush=True)

    spark = spark_session("TLCN-Bronze-Coffee-Trade-Monthly")

    try:
        merge_into_bronze(
            spark,
            TABLE,
            SCHEMA,
            rows,
            "coffee_trade_monthly_direct",
            "BRONZE_COFFEE_TRADE_MONTHLY",
        )

        spark.sql(f"""
            SELECT source_group,
                   COUNT(*) AS records,
                   COUNT(DISTINCT object_key) AS files
            FROM {TABLE}
            GROUP BY source_group
            ORDER BY source_group
        """).show(truncate=False)

        print("BRONZE_COFFEE_TRADE_MONTHLY_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
