"""MinIO -> Spark driver -> Iceberg Bronze. NO local JSONL export.
Run: .\run_spark.ps1 -Script 'src/iceberg/bronze_domestic_prices.py'
"""
from collections import Counter
from pyspark.sql.types import StructType, StructField, StringType, LongType
from minio_direct import (
    MinioDirect, csv_records, json_data_records, json_record,
    merge_into_bronze, record_id_for, spark_session,
)

TABLE = "lakehouse.bronze.domestic_prices_raw"
PREFIX = "bronze/domestic_price/"
SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("record_position", LongType(), False),
    StructField("record_type", StringType(), False),
    StructField("object_sha256", StringType(), False),
    StructField("source_layout", StringType(), False),
    StructField("raw_record_json", StringType(), False),
])


def main():
    import hashlib
    minio = MinioDirect()
    keys = sorted(k for k in minio.list_keys(PREFIX)
                  if k.lower().endswith((".csv", ".json")) and not k.endswith("/metadata.json"))
    if not keys:
        raise RuntimeError("No Domestic Price objects in MinIO")
    rows = []
    counts = Counter()
    for key in keys:
        raw = minio.read(key)
        digest = hashlib.sha256(raw).hexdigest()
        csv_file = key.lower().endswith(".csv")
        kind = "enriched_csv" if csv_file else "json_api_snapshot"
        iterator = csv_records(key, raw) if csv_file else json_data_records(key, raw)
        before = len(rows)
        for pos, record in enumerate(iterator, start=1):
            rows.append((
                record_id_for(key, pos), key, pos, kind, digest,
                "standard" if "/source=" in key else "legacy", json_record(record),
            ))
            counts[kind] += 1
        if len(rows) == before:
            counts["empty_objects"] += 1
    print(f"SOURCE_OBJECTS: {len(keys)}", flush=True)
    print(f"EMPTY_OBJECTS: {counts['empty_objects']}", flush=True)
    print(f"RECORDS_BY_TYPE: {{'enriched_csv': {counts['enriched_csv']}, 'json_api_snapshot': {counts['json_api_snapshot']}}}", flush=True)
    spark = spark_session("TLCN-Bronze-Domestic-Direct")
    try:
        merge_into_bronze(spark, TABLE, SCHEMA, rows, "domestic_direct", "BRONZE_DOMESTIC")
        spark.sql(f"""SELECT record_type, count(*) records, count(DISTINCT object_key) source_objects
            FROM {TABLE} GROUP BY record_type ORDER BY record_type""").show(truncate=False)
        print("BRONZE_DOMESTIC_PRICES_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
