"""Load append-only VICOFA JSONL snapshots from MinIO into Iceberg Bronze.

The crawler writes a complete JSONL snapshot pair when it discovers new
articles. This loader scans every snapshot, keeps the first observed version of
each article ID (immutable Bronze semantics), and inserts only unseen records.
It therefore remains idempotent as the VICOFA archive grows.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter

from pyspark.sql.types import LongType, StringType, StructField, StructType

from minio_direct import MinioDirect, json_record, merge_into_bronze, record_id_for, spark_session

TABLE = "lakehouse.bronze.vicofa_articles_raw"
PREFIX = "bronze/vicofa/source=vicofa_website/"
EXPECTED_FIELDS = {
    "articles.jsonl": {
        "id", "url", "title", "date", "summary", "content", "date_detail", "title_detail", "source"
    },
    "article_list.jsonl": {"id", "url", "title", "date", "summary"},
}
SCHEMA = StructType(
    [
        StructField("record_id", StringType(), False),
        StructField("object_key", StringType(), False),
        StructField("record_position", LongType(), False),
        StructField("domain", StringType(), False),
        StructField("source", StringType(), False),
        StructField("record_type", StringType(), False),
        StructField("object_sha256", StringType(), False),
        StructField("raw_record_json", StringType(), False),
    ]
)


def select_complete_snapshot_keys(all_keys: list[str]) -> list[str]:
    """Keep the first legacy list/detail objects plus only complete later pairs."""
    expected = {"article_list.jsonl", "articles.jsonl"}
    first_by_type: dict[str, str] = {}
    batches: dict[str, dict[str, str]] = {}
    for key in sorted(all_keys):
        filename = key.rsplit("/", 1)[-1]
        if filename not in expected:
            continue
        first_by_type.setdefault(filename, key)
        batch_segment = next(
            (part for part in key.split("/") if part.startswith("batch_id=")),
            None,
        )
        batch_id = batch_segment.split("=", 1)[1] if batch_segment else key.rsplit("/", 2)[0]
        batches.setdefault(batch_id, {})[filename] = key

    if set(first_by_type) != expected:
        raise RuntimeError("VICOFA MinIO checkpoint must contain at least one list and one detail object")

    selected = set(first_by_type.values())
    for files in batches.values():
        if set(files) == expected:
            selected.update(files.values())
    return sorted(selected)


def main() -> None:
    minio = MinioDirect()
    all_keys = sorted(
        key
        for key in minio.list_keys(PREFIX)
        if "/raw/" in key
        and key.endswith(("articles.jsonl", "article_list.jsonl"))
    )
    keys = select_complete_snapshot_keys(all_keys)
    ignored = len(all_keys) - len(keys)
    if ignored:
        print(f"VICOFA_INCOMPLETE_SNAPSHOT_OBJECTS_IGNORED: {ignored}", flush=True)
    if len(keys) < 2:
        raise RuntimeError(f"Expected at least one VICOFA list/detail snapshot pair; found {len(keys)} objects")

    rows = []
    by_type = Counter()
    unique_ids = {"vicofa_article_detail": set(), "vicofa_article_list": set()}
    first_record_by_id: dict[tuple[str, int], dict] = {}
    duplicate_versions = 0
    changed_duplicate_versions = 0
    object_counts = Counter()

    for index, key in enumerate(keys, start=1):
        filename = key.rsplit("/", 1)[-1]
        if filename not in EXPECTED_FIELDS:
            raise RuntimeError(f"Unexpected VICOFA object name: {key}")
        record_type = "vicofa_article_detail" if filename == "articles.jsonl" else "vicofa_article_list"
        raw = minio.read(key)
        digest = hashlib.sha256(raw).hexdigest()
        object_count = 0
        unique_in_object = 0

        for line_no, line in enumerate(raw.decode("utf-8-sig").splitlines(), start=1):
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

            object_count += 1
            identity = (record_type, record["id"])
            if identity in first_record_by_id:
                duplicate_versions += 1
                if first_record_by_id[identity] != record:
                    changed_duplicate_versions += 1
                continue

            first_record_by_id[identity] = record
            unique_ids[record_type].add(record["id"])
            rows.append(
                (
                    record_id_for(key, line_no),
                    key,
                    line_no,
                    "vicofa",
                    "vicofa_website",
                    record_type,
                    digest,
                    json_record(record),
                )
            )
            by_type[record_type] += 1
            unique_in_object += 1

        object_counts[key] = object_count
        print(
            f"VICOFA_SOURCE_OBJECT: {index}/{len(keys)} {filename} "
            f"rows={object_count}, first_seen_unique={unique_in_object}",
            flush=True,
        )

    detail_ids = unique_ids["vicofa_article_detail"]
    list_ids = unique_ids["vicofa_article_list"]
    if not detail_ids or not list_ids:
        raise RuntimeError(f"VICOFA list/detail data is empty: {dict(by_type)}")
    if detail_ids != list_ids:
        raise RuntimeError(
            "VICOFA list/detail IDs do not match; "
            f"missing_detail={sorted(list_ids-detail_ids)[:10]}, "
            f"missing_list={sorted(detail_ids-list_ids)[:10]}"
        )

    print(f"VICOFA_SOURCE_OBJECTS: {len(keys)}", flush=True)
    print(f"VICOFA_SOURCE_RECORDS_UNIQUE: {len(rows)}", flush=True)
    print(f"VICOFA_RECORDS_BY_TYPE: {dict(by_type)}", flush=True)
    print(f"VICOFA_DUPLICATE_VERSIONS_SKIPPED: {duplicate_versions}", flush=True)
    print(f"VICOFA_CHANGED_DUPLICATE_VERSIONS_KEPT_FIRST: {changed_duplicate_versions}", flush=True)

    spark = spark_session("TLCN-Bronze-VICOFA-Direct")
    try:
        merge_into_bronze(spark, TABLE, SCHEMA, rows, "vicofa_incoming", "BRONZE_VICOFA")
        total = spark.table(TABLE).count()
        if total != len(rows):
            raise RuntimeError(f"Unexpected Iceberg total {total}; expected {len(rows)} unique source records")
        spark.sql(
            f"""
            SELECT record_type, COUNT(*) AS records, COUNT(DISTINCT object_key) AS source_objects
            FROM {TABLE} GROUP BY record_type ORDER BY record_type
            """
        ).show(truncate=False)
        print("BRONZE_VICOFA_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
