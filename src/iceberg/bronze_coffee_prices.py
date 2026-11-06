"""MinIO -> Spark driver -> Iceberg Bronze. NO local JSONL export.
Run: .\run_spark.ps1 -Script 'src/iceberg/bronze_coffee_prices.py'
"""
from collections import Counter
from pyspark.sql.types import StructType, StructField, StringType, LongType
from minio_direct import (
    MinioDirect, json_data_records, json_record, merge_into_bronze,
    record_id_for, spark_session,
)

TABLE = "lakehouse.bronze.coffee_prices_raw"
PREFIX = "bronze/coffee_prices/source=bbw_coffee/"
SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("row_index", LongType(), True),
    StructField("batch_id", StringType(), True),
    StructField("ingestion_date", StringType(), True),
    StructField("source", StringType(), True),
    StructField("quote_date_vn", StringType(), True),
    StructField("product", StringType(), True),
    StructField("raw_record_json", StringType(), False),
])


def main():
    minio = MinioDirect()
    keys = sorted(k for k in minio.list_keys(PREFIX) if "/raw/" in k and k.lower().endswith(".json"))
    if not keys:
        raise RuntimeError("No BBW coffee raw JSON in MinIO")
    rows = []
    count = Counter()
    for key in keys:
        data = minio.read(key)
        parts = key.split("/")
        batch = next((p.split("=", 1)[1] for p in parts if p.startswith("batch_id=")), "")
        day = next((p.split("=", 1)[1] for p in parts if p.startswith("ingestion_date=")), "")
        length_before = len(rows)
        for index, record in enumerate(json_data_records(key, data)):
            rows.append((
                record_id_for(key, index), key, index, batch, day, "bbw_coffee",
                str(record.get("quote_date_vn") or ""), str(record.get("product") or ""),
                json_record(record),
            ))
        count["objects"] += 1
        if len(rows) == length_before:
            count["empty_objects"] += 1
    print(f"SOURCE_OBJECTS: {count['objects']}", flush=True)
    print(f"EMPTY_OBJECTS: {count['empty_objects']}", flush=True)
    spark = spark_session("TLCN-Bronze-BBW-Coffee-Direct")
    try:
        merge_into_bronze(spark, TABLE, SCHEMA, rows, "bbw_coffee_direct", "BRONZE_COFFEE")
        spark.sql(f"""SELECT source, count(*) records, count(DISTINCT object_key) source_objects
            FROM {TABLE} GROUP BY source""").show(truncate=False)
        print("BRONZE_COFFEE_PRICES_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
