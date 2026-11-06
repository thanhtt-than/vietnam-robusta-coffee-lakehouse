"""Load the two VICOFA JSONL raw objects directly from MinIO into Iceberg Bronze.

Place at D:/TLCN/src/iceberg/bronze_vicofa.py
Run: .\\run_spark.ps1 -Script 'src/iceberg/bronze_vicofa.py'
Requires the existing src/iceberg/minio_direct.py module.

Read-only on MinIO; writes only to the new Iceberg Bronze table.
Raw article text is preserved, including known crawler contamination.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter

from minio_direct import (
    MinioDirect,
    json_record,
    merge_into_bronze,
    record_id_for,
    spark_session,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

TABLE = "lakehouse.bronze.vicofa_articles_raw"
PREFIX = "bronze/vicofa/source=vicofa_website/"
EXPECTED_FILES = {"articles.jsonl": 1218, "article_list.jsonl": 1218}
EXPECTED_FIELDS = {
    "articles.jsonl": {"id", "url", "title", "date", "summary", "content", "date_detail", "title_detail", "source"},
    "article_list.jsonl": {"id", "url", "title", "date", "summary"},
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
    keys = sorted(k for k in minio.list_keys(PREFIX) if not k.endswith("/") and not k.endswith("/metadata.json"))
    if len(keys) != 2:
        raise RuntimeError(f"Expected 2 VICOFA JSONL objects, found {len(keys)}. Re-profile first: {keys}")

    filenames = [k.rsplit("/", 1)[-1] for k in keys]
    if set(filenames) != set(EXPECTED_FILES) or len(set(filenames)) != 2:
        raise RuntimeError(f"Unexpected VICOFA object names: {filenames}")
    if any("/raw/" not in k for k in keys):
        raise RuntimeError("Unexpected non-raw VICOFA object")

    rows = []
    by_type = Counter()
    for index, key in enumerate(keys, 1):
        filename = key.rsplit("/", 1)[-1]
        record_type = "vicofa_article_detail" if filename == "articles.jsonl" else "vicofa_article_list"
        raw = minio.read(key)
        digest = hashlib.sha256(raw).hexdigest()
        count = 0
        for line_no, line in enumerate(raw.decode("utf-8-sig").splitlines(), 1):
            if not line.strip():
                raise ValueError(f"Blank JSONL line at {key}:{line_no}; refusing ambiguous row numbering")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {key}:{line_no}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Not a JSON object at {key}:{line_no}")
            if set(record) != EXPECTED_FIELDS[filename]:
                raise ValueError(f"Unexpected fields at {key}:{line_no}: {sorted(record)}")
            if not isinstance(record["id"], int) or isinstance(record["id"], bool):
                raise ValueError(f"Invalid VICOFA ID at {key}:{line_no}")
            if not isinstance(record["url"], str) or not record["url"].startswith("https://vicofa.org.vn/"):
                raise ValueError(f"Invalid VICOFA URL at {key}:{line_no}")
            rows.append((
                record_id_for(key, line_no), key, line_no, "vicofa", "vicofa_website",
                record_type, digest, json_record(record),
            ))
            count += 1
            by_type[record_type] += 1
        print(f"VICOFA_SOURCE_OBJECT: {index}/{len(keys)} {filename} rows={count}", flush=True)
        if count != EXPECTED_FILES[filename]:
            raise RuntimeError(f"{filename}: expected {EXPECTED_FILES[filename]} records, got {count}")

    print(f"VICOFA_SOURCE_OBJECTS: {len(keys)}", flush=True)
    print(f"VICOFA_SOURCE_RECORDS: {len(rows)}", flush=True)
    print(f"VICOFA_RECORDS_BY_TYPE: {dict(by_type)}", flush=True)

    spark = spark_session("TLCN-Bronze-VICOFA-Direct")
    try:
        merge_into_bronze(spark, TABLE, SCHEMA, rows, "vicofa_incoming", "BRONZE_VICOFA")
        total = spark.table(TABLE).count()
        if total != len(rows):
            raise RuntimeError(f"Unexpected Iceberg total {total}; expected {len(rows)}")
        spark.sql(f"""
            SELECT record_type, COUNT(*) AS records, COUNT(DISTINCT object_key) AS source_objects
            FROM {TABLE} GROUP BY record_type ORDER BY record_type
        """).show(truncate=False)
        print("BRONZE_VICOFA_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
