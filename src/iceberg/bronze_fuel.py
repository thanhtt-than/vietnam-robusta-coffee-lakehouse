"""MinIO -> Spark driver -> Iceberg Bronze. NO local JSONL export.
Run: .\run_spark.ps1 -Script 'src/iceberg/bronze_fuel.py'
"""
import hashlib
import json
from collections import Counter
from pyspark.sql.types import StructType, StructField, StringType, LongType
from minio_direct import (
    MinioDirect, csv_records, json_record, merge_into_bronze,
    record_id_for, spark_session,
)

TABLE = "lakehouse.bronze.fuel_prices_raw"
PREFIXES = ("bronze/fuel/", "bronze/fuel_prices/")
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


def main():
    minio = MinioDirect()
    keys = sorted({k for prefix in PREFIXES for k in minio.list_keys(prefix)
                   if k.lower().endswith((".csv", ".json")) and not k.endswith("/metadata.json")})
    if not keys:
        raise RuntimeError("No Fuel objects in MinIO")
    rows = []
    counts = Counter()
    for key in keys:
        raw = minio.read(key)
        digest = hashlib.sha256(raw).hexdigest()
        domain = key.split("/")[1]
        source = next((p.split("=", 1)[1] for p in key.split("/") if p.startswith("source=")), "legacy_unknown")
        if key.lower().endswith(".csv"):
            kind = "legacy_csv"
            iterator = csv_records(key, raw)
        else:
            payload = json.loads(raw.decode("utf-8-sig"))
            if isinstance(payload, list):
                kind, iterator = "legacy_json_list", payload
            elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
                kind, iterator = "bbw_json_data", payload["data"]
            else:
                raise ValueError(f"Unsupported Fuel JSON layout: {key}")
        before = len(rows)
        for position, record in enumerate(iterator, start=1):
            if not isinstance(record, dict):
                raise ValueError(f"Non-object Fuel record: {key}")
            rows.append((
                record_id_for(key, position), key, position, domain, source,
                kind, digest, json_record(record),
            ))
            counts[kind] += 1
        if len(rows) == before:
            counts["empty_objects"] += 1
    print(f"SOURCE_OBJECTS: {len(keys)}", flush=True)
    print(f"EMPTY_OBJECTS: {counts['empty_objects']}", flush=True)
    print("RECORDS_BY_TYPE:", dict(counts), flush=True)
    spark = spark_session("TLCN-Bronze-Fuel-Direct")
    try:
        merge_into_bronze(spark, TABLE, SCHEMA, rows, "fuel_direct", "BRONZE_FUEL")
        spark.sql(f"""SELECT domain, record_type, count(*) records,
            count(DISTINCT object_key) source_objects FROM {TABLE}
            GROUP BY domain, record_type ORDER BY domain, record_type""").show(truncate=False)
        print("BRONZE_FUEL_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
