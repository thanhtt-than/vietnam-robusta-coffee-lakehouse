"""Normalize monthly coffee exports from Bronze to Silver Iceberg."""

import hashlib
import json
import re
import unicodedata
from collections import Counter
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, DecimalType
)
from minio_direct import spark_session

BRONZE = "lakehouse.bronze.coffee_trade_monthly_raw"
SILVER = "lakehouse.silver.coffee_trade_monthly"

SCHEMA = StructType([
    StructField("observation_id", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("market_code", StringType(), False),
    StructField("market_name", StringType(), False),
    StructField("market_type", StringType(), False),
    StructField("quantity_tonnes", DecimalType(20, 3), True),
    StructField("value_usd_thousand", DecimalType(20, 3), True),
    StructField("source_group", StringType(), False),
    StructField("bronze_record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("record_position", LongType(), False),
    StructField("quality_status", StringType(), False),
])


def norm(value):
    value = unicodedata.normalize("NFD", str(value).casefold())
    value = "".join(
        ch for ch in value
        if unicodedata.category(ch) != "Mn"
    )
    return " ".join(value.replace("\u0111", "d").split())


def coffee_row(cells):
    return any(norm(cell) == "ca phe" for cell in cells[:3])


def number(value):
    text = str(value).strip()
    text = re.sub(r"[\s\u00a0\u202f]", "", text)
    if text in ("", "-", "..."):
        return None
    if not re.fullmatch(r"\d+", text):
        raise ValueError("Unexpected numeric value: " + repr(value))
    return Decimal(text)


def month_columns(cells):
    found = []
    for col, cell in enumerate(cells):
        match = re.search(
            r"\bthang\s*0?([1-9]|1[0-2])\b",
            norm(cell)
        )
        if match:
            found.append((int(match.group(1)), col))
    return found


def market_code(name):
    text = norm(name)
    code = re.sub(r"[^A-Z0-9]+", "_", text.upper()).strip("_")
    if not code:
        raise ValueError("Empty market code: " + repr(name))
    return code


def build_rows(bronze_rows):
    result = []
    reference = {}
    primary = {}
    counts = Counter()
    missing_market_pairs = 0

    groups = {}

    for item in bronze_rows:
        groups.setdefault(item["object_key"], []).append(item)

    if len(groups) != 36:
        raise RuntimeError(f"Expected 36 source files, got {len(groups)}")

    for object_key, records in sorted(groups.items()):
        records.sort(key=lambda r: r["record_position"])
        source_group = records[0]["source_group"]
        year = int(records[0]["source_year"])

        if any(
            r["source_group"] != source_group
            or int(r["source_year"]) != year
            for r in records
        ):
            raise RuntimeError("Mixed metadata in " + object_key)

        parsed = [
            (r, json.loads(r["raw_record_json"])["cells"])
            for r in records
        ]

        header = None
        header_index = None

        for i, (_, cells) in enumerate(parsed[:8]):
            candidate = month_columns(cells)
            if len(candidate) >= 9:
                header = candidate
                header_index = i
                break

        if header is None:
            raise RuntimeError("Missing monthly header: " + object_key)

        expected_months = list(range(
            1, 10 if year == 2026 else 13
        ))

        if [m for m, _ in header] != expected_months:
            raise RuntimeError("Unexpected months: " + object_key)

        current_market = None
        coffee_count = 0

        for record, cells in parsed[header_index + 2:]:
            if source_group == "export_value_by_market":
                if cells and cells[0].strip():
                    current_market = cells[0].strip()

            if not coffee_row(cells):
                continue

            coffee_count += 1

            if source_group == "export_value_by_market":
                if not current_market:
                    raise RuntimeError("Missing market: " + object_key)
                name = current_market
                code = market_code(name)
                market_type = (
                    "regional_group"
                    if code in ("EU", "ASEAN")
                    else "country"
                )
            else:
                name = "ALL_MARKETS"
                code = "ALL_MARKETS"
                market_type = "national_total"

            for month, col in header:
                quantity = number(
                    cells[col] if col < len(cells) else ""
                )
                value = number(
                    cells[col + 1] if col + 1 < len(cells) else ""
                )

                if quantity is None and value is None:
                    if source_group == "export_value_by_market":
                        missing_market_pairs += 1
                    continue

                if quantity is not None and quantity < 0:
                    raise RuntimeError("Negative quantity")
                if value is not None and value < 0:
                    raise RuntimeError("Negative value")

                key = (year, month)

                if source_group in (
                    "export_commodity_total",
                    "export_commodity_by_market",
                ):
                    destination = (
                        primary if source_group == "export_commodity_total"
                        else reference
                    )
                    if key in destination:
                        raise RuntimeError(
                            "Duplicate national observation: " + str(key)
                        )
                    destination[key] = (quantity, value)

                if source_group == "export_commodity_by_market":
                    continue

                identity = f"{record['record_id']}||{month}"
                observation_id = hashlib.sha256(
                    identity.encode("utf-8")
                ).hexdigest()

                status = (
                    "complete"
                    if quantity is not None and value is not None
                    else "partial"
                )

                result.append((
                    observation_id,
                    year,
                    month,
                    code,
                    name,
                    market_type,
                    quantity,
                    value,
                    source_group,
                    record["record_id"],
                    object_key,
                    int(record["record_position"]),
                    status,
                ))
                counts[source_group] += 1

        if source_group != "export_value_by_market" and coffee_count != 1:
            raise RuntimeError("Invalid national coffee rows: " + object_key)

        if source_group == "export_value_by_market" and not 8 <= coffee_count <= 9:
            raise RuntimeError("Invalid market coffee rows: " + object_key)

    if len(primary) != 141 or len(reference) != 141:
        raise RuntimeError(
            f"National source counts: {len(primary)}, {len(reference)}"
        )

    if set(primary) != set(reference):
        raise RuntimeError("National source month keys differ")

    mismatches = [
        (key, primary[key], reference[key])
        for key in sorted(primary)
        if primary[key] != reference[key]
    ]

    print("NATIONAL_REFERENCE_MISMATCHES:", len(mismatches), flush=True)
    for item in mismatches[:10]:
        print("NATIONAL_REFERENCE_DIFFERENCE:", item, flush=True)

    print("SILVER_SOURCE_COUNTS:", dict(counts), flush=True)
    print("MARKET_MISSING_PAIRS:", missing_market_pairs, flush=True)

    if counts["export_commodity_total"] != 141:
        raise RuntimeError("Incorrect national observation count")
    if counts["export_value_by_market"] != 1171:
        raise RuntimeError("Incorrect market observation count")
    if missing_market_pairs != 38:
        raise RuntimeError("Unexpected missing market pairs")

    business_keys = [
        (r[1], r[2], r[3]) for r in result
    ]
    if len(business_keys) != len(set(business_keys)):
        raise RuntimeError("Duplicate year/month/market keys")

    return result


def main():
    write = "--write" in __import__("sys").argv[1:]
    spark = spark_session("TLCN-Silver-Coffee-Trade-Monthly")

    try:
        bronze = spark.table(BRONZE)
        source = bronze.collect()

        print("BRONZE_ROWS:", len(source), flush=True)

        if len(source) != 12029:
            raise RuntimeError("Unexpected Bronze row count")

        records = build_rows(source)
        df = spark.createDataFrame(records, SCHEMA).cache()

        try:
            total = df.count()
            unique = df.select("observation_id").distinct().count()

            print("SILVER_CANDIDATE_ROWS:", total, flush=True)
            print("SILVER_UNIQUE_IDS:", unique, flush=True)

            if total != unique or total != 1312:
                raise RuntimeError("Silver candidate validation failed")

            if not write:
                print("SILVER_TRADE_DRY_RUN_PASSED", flush=True)
                return

            spark.sql(
                "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver"
            )

            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {SILVER} (
                    observation_id STRING,
                    year INT,
                    month INT,
                    market_code STRING,
                    market_name STRING,
                    market_type STRING,
                    quantity_tonnes DECIMAL(20,3),
                    value_usd_thousand DECIMAL(20,3),
                    source_group STRING,
                    bronze_record_id STRING,
                    object_key STRING,
                    record_position BIGINT,
                    quality_status STRING
                ) USING iceberg
            """)

            stored = spark.table(SILVER)

            if set(stored.columns) != set(SCHEMA.names):
                raise RuntimeError("Existing Silver schema differs")

            joined = df.alias("a").join(
                stored.alias("b"),
                on="observation_id",
                how="inner"
            )

            mismatch = None
            for field in SCHEMA.names:
                if field == "observation_id":
                    continue
                term = ~F.col("a." + field).eqNullSafe(
                    F.col("b." + field)
                )
                mismatch = term if mismatch is None else mismatch | term

            if joined.filter(mismatch).limit(1).count():
                raise RuntimeError("Existing Silver observation differs")

            new_count = df.select("observation_id").join(
                stored.select("observation_id"),
                on="observation_id",
                how="left_anti"
            ).count()

            print("SILVER_NEW_RECORDS:", new_count, flush=True)

            if new_count:
                df.createOrReplaceTempView("trade_monthly_incoming")
                columns = ", ".join(SCHEMA.names)
                values = ", ".join(
                    "s." + name for name in SCHEMA.names
                )

                spark.sql(f"""
                    MERGE INTO {SILVER} t
                    USING trade_monthly_incoming s
                    ON t.observation_id = s.observation_id
                    WHEN NOT MATCHED THEN
                      INSERT ({columns}) VALUES ({values})
                """)

            saved = spark.table(SILVER)
            saved_count = saved.count()
            print("SILVER_TRADE_TOTAL:", saved_count, flush=True)

            if saved_count != 1312:
                raise RuntimeError("Incorrect Silver total")

            print("SILVER_COFFEE_TRADE_MONTHLY_PASSED", flush=True)

        finally:
            df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
