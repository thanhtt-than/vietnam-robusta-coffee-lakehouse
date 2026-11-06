"""Load area, production, and trade CSV raw objects directly from MinIO into Iceberg Bronze.

Install: D:/TLCN/src/iceberg/bronze_historical_stats.py
Run: .\\run_spark.ps1 -Script 'src/iceberg/bronze_historical_stats.py'
Requires: src/iceberg/minio_direct.py and the standard Spark Iceberg runtime.

Read-only against MinIO. No local JSONL staging. All source copies are preserved
as distinct records; duplicate business observations are addressed in Silver.
"""
from __future__ import annotations

import hashlib
from collections import Counter

from minio_direct import (
    MinioDirect,
    csv_records,
    json_record,
    merge_into_bronze,
    record_id_for,
    spark_session,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

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

DOMAINS = {
    "area": {
        "table": "lakehouse.bronze.coffee_area_raw",
        "record_type": "coffee_area_csv",
        "columns": {"year", "coffee_area_thousand_ha", "coffee_area_growth_index_prev_year_100", "status"},
        "expected_objects": 2,
        "expected_rows": 38,
    },
    "production": {
        "table": "lakehouse.bronze.coffee_production_raw",
        "record_type": "coffee_production_csv",
        "columns": {"year", "coffee_production_thousand_tons", "coffee_production_growth_index_prev_year_100", "status"},
        "expected_objects": 3,
        "expected_rows": 57,
    },
    "trade": {
        "table": "lakehouse.bronze.coffee_trade_raw",
        "record_type": "coffee_trade_csv",
        "columns": {"reporterCd", "reporterLabel", "partnerCd", "partnerLabel", "productCd", "productLabel"}
            | {f"{year} (USD Thousand)" for year in range(2001, 2025)},
        "expected_objects": 2,
        "expected_rows": 324,
    },
}


def classify_source(key: str) -> str:
    return next((piece.split("=", 1)[1] for piece in key.split("/") if piece.startswith("source=")), "legacy_unknown")


def data_keys(minio: MinioDirect, domain: str) -> list[str]:
    prefix = f"bronze/{domain}/"
    keys = sorted(k for k in minio.list_keys(prefix) if not k.endswith("/") and not k.endswith("/metadata.json"))
    unexpected = [k for k in keys if not k.lower().endswith(".csv") or ("/source=" in k and "/raw/" not in k)]
    if unexpected:
        raise RuntimeError(f"Unexpected objects in {prefix}: {unexpected}")
    return keys


def run_domain(spark, minio, domain: str, config: dict):
    keys = data_keys(minio, domain)
    if len(keys) != config["expected_objects"]:
        raise RuntimeError(f"{domain}: expected {config['expected_objects']} objects, got {len(keys)}. Re-profile before ingestion.")

    rows = []
    hashes = Counter()
    for i, key in enumerate(keys, 1):
        raw = minio.read(key)
        digest = hashlib.sha256(raw).hexdigest()
        hashes[digest] += 1
        source = classify_source(key)
        n = 0
        for position, record in enumerate(csv_records(key, raw), start=1):
            if set(record) != config["columns"]:
                raise RuntimeError(f"{domain}: unexpected CSV columns at {key}: {sorted(record)}")
            rows.append((
                record_id_for(key, position), key, position,
                domain, source, config["record_type"], digest, json_record(record),
            ))
            n += 1
        print(f"{domain.upper()}_OBJECT: {i}/{len(keys)} rows={n} source={source}", flush=True)

    if len(rows) != config["expected_rows"]:
        raise RuntimeError(f"{domain}: expected {config['expected_rows']} rows, got {len(rows)}. Re-profile before ingestion.")
    print(f"{domain.upper()}_SOURCE_OBJECTS: {len(keys)}", flush=True)
    print(f"{domain.upper()}_SOURCE_RECORDS: {len(rows)}", flush=True)
    print(f"{domain.upper()}_UNIQUE_FILE_CONTENTS: {len(hashes)}", flush=True)

    merge_into_bronze(
        spark, config["table"], SCHEMA, rows,
        f"{domain}_incoming", f"BRONZE_{domain.upper()}",
    )
    final = spark.table(config["table"]).count()
    if final != len(rows):
        raise RuntimeError(f"{domain}: Iceberg table has {final} rows, source has {len(rows)}. Investigate before proceeding.")
    spark.sql(f"""
        SELECT source, count(*) AS records, count(DISTINCT object_key) AS source_objects
        FROM {config['table']}
        GROUP BY source ORDER BY source
    """).show(truncate=False)
    print(f"BRONZE_{domain.upper()}_PASSED", flush=True)


def main():
    minio = MinioDirect()
    spark = spark_session("TLCN-Bronze-Historical-Stats-Direct")
    try:
        for domain, config in DOMAINS.items():
            print(f"===== DOMAIN: {domain.upper()} =====", flush=True)
            run_domain(spark, minio, domain, config)
        print("BRONZE_HISTORICAL_STATS_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
