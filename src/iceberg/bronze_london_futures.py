"""Load London coffee futures CSV raw objects from MinIO directly into Iceberg Bronze.

Install: D:/TLCN/src/iceberg/bronze_london_futures.py
Requires: D:/TLCN/src/iceberg/minio_direct.py (existing module).
Run: .\\run_spark.ps1 -Script 'src/iceberg/bronze_london_futures.py'

Read-only to MinIO. Never normalizes/deduplicates business rows in Bronze.
Both original object locations are retained, even when bytes are identical.
"""
from __future__ import annotations

import hashlib
from collections import Counter

from pyspark.sql.types import LongType, StringType, StructField, StructType
from minio_direct import (
    MinioDirect, csv_records, json_record, merge_into_bronze,
    record_id_for, spark_session,
)

PREFIX = "bronze/london_futures/"
TABLE = "lakehouse.bronze.london_futures_raw"
BATCH_ROWS = 2500
EXPECTED_COLUMNS = {
    "Ngày", "Lần cuối", "Mở", "Cao", "Thấp", "KL", "% Thay đổi",
}
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
    keys = sorted(
        k for k in minio.list_keys(PREFIX)
        if k.lower().endswith(".csv") and "/raw/" in k or
        (k.lower().endswith(".csv") and k.startswith(PREFIX) and "/source=" not in k)
    )
    # Exclude non-CSV metadata and ensure there are no unsupported data objects.
    all_data_keys = sorted(
        k for k in minio.list_keys(PREFIX)
        if not k.endswith("/metadata.json") and not k.endswith("/")
        and ("/raw/" in k or "/source=" not in k)
    )
    unsupported = set(all_data_keys) - set(keys)
    if unsupported:
        raise RuntimeError(f"Unsupported London Futures data object(s): {sorted(unsupported)}")
    if not keys:
        raise RuntimeError("No London Futures CSV objects in MinIO")

    print(f"LONDON_SOURCE_OBJECTS: {len(keys)}", flush=True)
    spark = spark_session("TLCN-Bronze-London-Futures-Direct")
    buffer = []
    source_total = 0
    source_hashes = {}
    try:
        for object_index, key in enumerate(keys, start=1):
            raw = minio.read(key)
            digest = hashlib.sha256(raw).hexdigest()
            source_hashes[key] = digest
            source = next((p.split("=", 1)[1] for p in key.split("/")
                           if p.startswith("source=")), "legacy_unknown")
            object_rows = 0
            for position, record in enumerate(csv_records(key, raw), start=1):
                if set(record) != EXPECTED_COLUMNS:
                    raise ValueError(
                        f"Unexpected CSV columns in {key}: {sorted(record)}"
                    )
                buffer.append((
                    record_id_for(key, position), key, position,
                    "london_futures", source, "london_futures_csv",
                    digest, json_record(record),
                ))
                object_rows += 1
                source_total += 1
                if len(buffer) >= BATCH_ROWS:
                    merge_into_bronze(
                        spark, TABLE, SCHEMA, buffer,
                        "london_futures_incoming", "BRONZE_LONDON_BATCH",
                    )
                    buffer.clear()
            print(f"SOURCE_OBJECT: {object_index}/{len(keys)} rows={object_rows} source={source}", flush=True)
        if buffer:
            merge_into_bronze(
                spark, TABLE, SCHEMA, buffer,
                "london_futures_incoming", "BRONZE_LONDON_BATCH",
            )
            buffer.clear()

        total_iceberg = spark.table(TABLE).count()
        print(f"LONDON_SOURCE_RECORDS: {source_total}", flush=True)
        print(f"LONDON_UNIQUE_CONTENTS: {len(set(source_hashes.values()))}", flush=True)
        print(f"BRONZE_LONDON_TOTAL: {total_iceberg}", flush=True)
        if total_iceberg != source_total:
            raise RuntimeError(
                f"Iceberg has {total_iceberg} records, MinIO yielded {source_total}; "
                "investigate changed/removed source objects before continuing"
            )
        spark.sql(f"""
            SELECT source, record_type, count(*) AS records,
                   count(DISTINCT object_key) AS source_objects
            FROM {TABLE} GROUP BY source, record_type ORDER BY source
        """).show(truncate=False)
        print("BRONZE_LONDON_FUTURES_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
