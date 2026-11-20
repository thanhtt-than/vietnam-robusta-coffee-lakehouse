"""Silver exchange rate: validate and normalize Bronze observations.

Dry run:
  run_spark.ps1 -Script src/iceberg/silver_exchange_rate.py
"""

import sys
import json
import re
import hashlib
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation

from minio_direct import spark_session

SOURCE = "lakehouse.bronze.exchange_rate_raw"

IMF_SERIES = {
    "VNM.XDC_USD.PA_RT.M": ("USD/VND", "PERIOD_AVERAGE"),
    "VNM.XDC_USD.EOP_RT.M": ("USD/VND", "END_OF_PERIOD"),
    "VNM.XDC_EUR.PA_RT.M": ("EUR/VND", "PERIOD_AVERAGE"),
    "VNM.XDC_EUR.EOP_RT.M": ("EUR/VND", "END_OF_PERIOD"),
}


def parse_vnd(value, vietnamese=False):
    text = str(value or "").strip()
    text = re.sub(r"\s*VNĐ\s*$", "", text, flags=re.I).strip()

    if vietnamese:
        text = text.replace(".", "").replace(",", ".")
    else:
        if re.fullmatch(r"\d+,\d+", text):
            text = text.replace(",", ".")
        elif not re.fullmatch(r"\d+(?:\.\d+)?", text):
            raise ValueError(f"Invalid numeric rate: {text}")

    number = Decimal(text)
    if not number.is_finite() or number <= 0:
        raise ValueError(f"Invalid positive rate: {text}")
    return number


def observation_id(record_id, period=""):
    return hashlib.sha256(
        (record_id + "|" + period).encode("utf-8")
    ).hexdigest()


def normalize(row):
    raw = json.loads(row.raw_record_json)
    source = row.source
    result = []

    base = {
        "source": source,
        "bronze_record_id": row.record_id,
        "object_key": row.object_key,
    }

    if source in ("usd_vnd_csv", "eur_vnd_csv"):
        pair = "USD/VND" if source.startswith("usd") else "EUR/VND"

        day = datetime.strptime(
            raw["Date"].strip(), "%d/%m/%Y %H:%M:%S"
        ).date()

        rate = parse_vnd(raw["Close"])

        result.append({
            **base,
            "observation_id": observation_id(row.record_id),
            "currency_pair": pair,
            "observation_date": day.isoformat(),
            "frequency": "DAILY",
            "rate_type": "CLOSE",
            "rate_vnd": str(rate),
        })

    elif source in ("usd_vnd_excel", "eur_vnd_excel"):
        pair = "USD/VND" if source.startswith("usd") else "EUR/VND"

        expected = pair.split("/")[0]
        if raw.get("column_1", "").strip().upper() != expected:
            raise ValueError("Excel currency code mismatch")

        day = datetime.strptime(
            raw["Ngày hiệu lực"].strip(), "%d/%m/%Y"
        ).date()

        rate = parse_vnd(raw["Tỷ giá"], vietnamese=True)

        result.append({
            **base,
            "observation_id": observation_id(row.record_id),
            "currency_pair": pair,
            "observation_date": day.isoformat(),
            "frequency": "DAILY",
            "rate_type": "PUBLISHED",
            "rate_vnd": str(rate),
        })

    elif source == "imf_exchange_rate":
        series = raw.get("SERIES_CODE", "")

        if series not in IMF_SERIES:
            return []

        pair, rate_type = IMF_SERIES[series]

        for period, value in raw.items():
            if not re.fullmatch(r"\d{4}-M(0[1-9]|1[0-2])", period):
                continue

            if value is None or str(value).strip() == "":
                continue

            day = datetime.strptime(
                period.replace("-M", "-"), "%Y-%m"
            ).date()

            rate = parse_vnd(value)

            result.append({
                **base,
                "observation_id": observation_id(row.record_id, period),
                "currency_pair": pair,
                "observation_date": day.isoformat(),
                "frequency": "MONTHLY",
                "rate_type": rate_type,
                "rate_vnd": str(rate),
            })

    else:
        raise ValueError(f"Unsupported source: {source}")

    return result


def write_silver(spark, observations):
    from pyspark.sql import functions as F

    target_name = "lakehouse.silver.exchange_rate_observations"

    # Convert normalized strings into typed Iceberg columns.
    df = spark.createDataFrame(observations).select(
        F.col("observation_id"),
        F.col("currency_pair"),
        F.to_date("observation_date").alias("observation_date"),
        F.col("frequency"),
        F.col("rate_type"),
        F.col("rate_vnd").cast("decimal(28,12)").alias("rate_vnd"),
        F.col("source"),
        F.col("bronze_record_id"),
        F.col("object_key"),
    ).cache()

    try:
        count = df.count()

        if count != 8166:
            raise RuntimeError(
                f"Expected 8166 Silver observations, found {count}"
            )

        if df.filter(
            "observation_date IS NULL OR rate_vnd IS NULL OR rate_vnd <= 0"
        ).limit(1).count():
            raise RuntimeError("Invalid typed Silver observations")

        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver"
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_name} (
                observation_id STRING,
                currency_pair STRING,
                observation_date DATE,
                frequency STRING,
                rate_type STRING,
                rate_vnd DECIMAL(28,12),
                source STRING,
                bronze_record_id STRING,
                object_key STRING
            ) USING iceberg
        """)

        existing = spark.table(target_name)

        if set(existing.columns) != set(df.columns):
            raise RuntimeError("Existing Silver table schema mismatch")

        comparison = df.alias("incoming").join(
            existing.alias("stored"),
            on="observation_id",
            how="inner"
        )

        mismatch = None

        for name in df.columns:
            if name == "observation_id":
                continue

            difference = ~F.col(
                f"incoming.{name}"
            ).eqNullSafe(F.col(f"stored.{name}"))

            mismatch = (
                difference if mismatch is None
                else mismatch | difference
            )

        if comparison.filter(mismatch).limit(1).count():
            raise RuntimeError(
                "Existing Silver observations differ; refusing overwrite"
            )

        new_count = df.select("observation_id").join(
            existing.select("observation_id"),
            on="observation_id",
            how="left_anti"
        ).count()

        print("SILVER_NEW_RECORDS:", new_count, flush=True)

        if new_count:
            df.createOrReplaceTempView("exchange_rate_silver_stage")

            spark.sql(f"""
                MERGE INTO {target_name} AS t
                USING exchange_rate_silver_stage AS s
                ON t.observation_id = s.observation_id
                WHEN NOT MATCHED THEN INSERT *
            """)

        final = spark.table(target_name)
        total = final.count()
        unique = final.select("observation_id").distinct().count()

        print("SILVER_EXCHANGE_RATE_TOTAL:", total, flush=True)
        print("SILVER_UNIQUE_IDS:", unique, flush=True)

        if total != count or unique != total:
            raise RuntimeError("Silver count or uniqueness mismatch")

        final.groupBy(
            "currency_pair", "frequency", "rate_type"
        ).count().orderBy(
            "currency_pair", "frequency", "rate_type"
        ).show(truncate=False)

        print("SILVER_EXCHANGE_RATE_ICEBERG_PASSED", flush=True)

    finally:
        df.unpersist()

def main():
    spark = spark_session("TLCN-Silver-Exchange-Rate")

    try:
        bronze = spark.table(SOURCE).select(
            "record_id", "object_key", "source", "raw_record_json"
        )

        rows = bronze.collect()
        print("BRONZE_RECORDS:", len(rows), flush=True)

        if len(rows) != 7476:
            raise RuntimeError("Unexpected Bronze row count")

        observations = []
        rejected = []
        counts = Counter()

        for row in rows:
            try:
                converted = normalize(row)
                observations.extend(converted)
                counts[row.source] += len(converted)

            except (ValueError, KeyError, InvalidOperation) as exc:
                rejected.append((row.source, row.record_id, str(exc)))

        print("=== SILVER NORMALIZATION ===", flush=True)

        for source, count in sorted(counts.items()):
            print(f"{source}: {count}", flush=True)

        print("NORMALIZED_OBSERVATIONS:", len(observations), flush=True)
        print("REJECTED_RECORDS:", len(rejected), flush=True)

        for item in rejected[:10]:
            print("REJECTED_SAMPLE:", item, flush=True)

        ids = [r["observation_id"] for r in observations]
        print("UNIQUE_OBSERVATION_IDS:", len(set(ids)), flush=True)

        if len(ids) != len(set(ids)):
            raise RuntimeError("Duplicate observation IDs")

        if counts["imf_exchange_rate"] != 702:
            raise RuntimeError("Unexpected IMF observation count")

        business_keys = defaultdict(list)

        for r in observations:
            key = (
                r["source"],
                r["currency_pair"],
                r["observation_date"],
                r["frequency"],
                r["rate_type"],
            )
            business_keys[key].append(r)

        duplicate_groups = {
            key: values
            for key, values in business_keys.items()
            if len(values) > 1
        }

        conflicting_groups = sum(
            len({v["rate_vnd"] for v in values}) > 1
            for values in duplicate_groups.values()
        )

        print("DUPLICATE_BUSINESS_KEYS:", len(duplicate_groups), flush=True)
        print("CONFLICTING_RATE_GROUPS:", conflicting_groups, flush=True)

        print("=== OBSERVATIONS BY PAIR AND FREQUENCY ===", flush=True)

        grouped = Counter(
            (r["currency_pair"], r["frequency"], r["rate_type"])
            for r in observations
        )

        for key, count in sorted(grouped.items()):
            print(f"{key}: {count}", flush=True)

        if not observations:
            raise RuntimeError("No valid observations")

        if "--write" in sys.argv[1:]:
            if rejected or duplicate_groups or conflicting_groups:
                raise RuntimeError("Silver validation failed; refusing write")

            write_silver(spark, observations)
        else:
            print("SILVER_EXCHANGE_RATE_DRY_RUN_PASSED", flush=True)
            print("NO_SILVER_TABLE_WRITTEN", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
