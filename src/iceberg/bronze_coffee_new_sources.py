"""
TLCN - Ingest 7 official-statistics DOCX + 2 USDA PSD CSV objects
from MinIO Raw into Bronze Iceberg.

The legacy psd.xls object is intentionally retained in MinIO but excluded
from this ingestion because it is HTML disguised as XLS and has been replaced
for processing by the uploaded psd.csv.

Default: DRY RUN (no Iceberg writes)
Write:   pass --write after reviewing the dry-run output.

Run from D:\\TLCN:
    .\\run_spark.ps1 -Script "src/iceberg/bronze_coffee_new_sources.py"

After dry-run is reviewed:
    .\\run_spark.ps1 -Script "src/iceberg/bronze_coffee_new_sources.py" -ScriptArgs "--write"
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import zipfile
from collections import Counter
from pathlib import PurePosixPath
from xml.etree import ElementTree as ET

from minio_direct import (
    MinioDirect,
    json_record,
    record_id_for,
    spark_session,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType


STATISTICS_PREFIX = "bronze/coffee_statistics/"
PSD_PREFIX = "bronze/coffee_psd/"

STATISTICS_TABLE = "lakehouse.bronze.coffee_statistics_raw"
PSD_TABLE = "lakehouse.bronze.coffee_psd_raw"

EXPECTED_DOCX_OBJECTS = 7
EXPECTED_PSD_CSV_OBJECTS = {"psd_coffee.csv", "psd.csv"}
EXPECTED_LEGACY_XLS_OBJECTS = {"psd.xls"}

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


def source_from_key(key: str) -> str:
    for part in key.split("/"):
        if part.startswith("source="):
            return part.split("=", 1)[1]
    return "unknown"


def metadata_key_for(raw_key: str) -> str:
    if "/raw/" not in raw_key:
        raise ValueError(f"Not a raw object key: {raw_key}")
    return raw_key.rsplit("/raw/", 1)[0] + "/metadata.json"


def verify_metadata(minio: MinioDirect, key: str, raw: bytes) -> str:
    digest = hashlib.sha256(raw).hexdigest()
    metadata = json.loads(minio.read(metadata_key_for(key)).decode("utf-8"))

    expected = {
        "sha256": digest,
        "size_bytes": len(raw),
        "raw_object_key": key,
        "raw_filename": PurePosixPath(key).name,
        "status": "success",
    }
    mismatches = {
        field: {"expected": value, "actual": metadata.get(field)}
        for field, value in expected.items()
        if metadata.get(field) != value
    }
    if mismatches:
        raise RuntimeError(
            f"Metadata validation failed for {key}: {mismatches}"
        )
    return digest


def list_raw_keys(minio: MinioDirect, prefix: str) -> list[str]:
    return sorted(
        key for key in minio.list_keys(prefix)
        if "/raw/" in key and not key.endswith("/")
    )


def word_cell_text(cell) -> str:
    parts = []
    for node in cell.iter():
        if node.tag.endswith("}t") and node.text:
            parts.append(node.text)
        elif node.tag.endswith("}tab"):
            parts.append("\t")
        elif node.tag.endswith("}br"):
            parts.append("\n")
    return "".join(parts).strip()


def parse_docx_rows(key: str, raw: bytes) -> list[dict]:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            document_xml = archive.read("word/document.xml")
    except Exception as exc:
        raise RuntimeError(f"Cannot read DOCX {key}: {exc}") from exc

    root = ET.fromstring(document_xml)
    tables = [node for node in root.iter() if node.tag.endswith("}tbl")]
    if not tables:
        raise RuntimeError(f"No Word tables found in {key}")

    extracted = []
    position = 0

    for table_index, table in enumerate(tables, start=1):
        rows = [node for node in table if node.tag.endswith("}tr")]
        for row_index, row in enumerate(rows, start=1):
            cells = [
                word_cell_text(cell)
                for cell in row
                if cell.tag.endswith("}tc")
            ]
            if not cells:
                continue

            position += 1
            extracted.append({
                "table_index": table_index,
                "row_index": row_index,
                "cells": cells,
                "cell_count": len(cells),
            })

    if not extracted:
        raise RuntimeError(f"No table rows extracted from {key}")
    return extracted


def parse_csv_rows(key: str, raw: bytes) -> list[dict]:
    text = raw.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text, newline=""))

    try:
        header = next(reader)
    except StopIteration:
        raise RuntimeError(f"Empty CSV: {key}")

    header = [name.strip() for name in header]
    if not header or any(not name for name in header):
        raise RuntimeError(f"CSV has blank column names: {key}")
    if len(header) != len(set(header)):
        raise RuntimeError(f"CSV has duplicate column names: {key}")

    extracted = []
    for row_index, values in enumerate(reader, start=1):
        if not values or (len(values) == 1 and not values[0].strip()):
            continue
        if len(values) != len(header):
            raise RuntimeError(
                f"CSV column count mismatch in {key}, row {row_index}: "
                f"expected {len(header)}, got {len(values)}"
            )

        extracted.append({
            "row_index": row_index,
            "columns": header,
            "values": {
                name: value.strip()
                for name, value in zip(header, values)
            },
        })

    if not extracted:
        raise RuntimeError(f"CSV has no data rows: {key}")
    return extracted


def selected_source_objects(minio: MinioDirect):
    statistics_keys = list_raw_keys(minio, STATISTICS_PREFIX)
    psd_all_keys = list_raw_keys(minio, PSD_PREFIX)

    docx_keys = [
        key for key in statistics_keys
        if PurePosixPath(key).suffix.lower() == ".docx"
    ]
    other_statistics = sorted(set(statistics_keys) - set(docx_keys))

    psd_csv_keys = [
        key for key in psd_all_keys
        if PurePosixPath(key).suffix.lower() == ".csv"
    ]
    psd_xls_keys = [
        key for key in psd_all_keys
        if PurePosixPath(key).suffix.lower() == ".xls"
    ]
    other_psd = sorted(
        set(psd_all_keys) - set(psd_csv_keys) - set(psd_xls_keys)
    )

    if other_statistics:
        raise RuntimeError(
            f"Unexpected objects in coffee_statistics: {other_statistics}"
        )
    if other_psd:
        raise RuntimeError(f"Unexpected objects in coffee_psd: {other_psd}")

    if len(docx_keys) != EXPECTED_DOCX_OBJECTS:
        raise RuntimeError(
            f"Expected {EXPECTED_DOCX_OBJECTS} DOCX objects, found {len(docx_keys)}"
        )

    csv_names = {PurePosixPath(key).name for key in psd_csv_keys}
    if csv_names != EXPECTED_PSD_CSV_OBJECTS or len(psd_csv_keys) != 2:
        raise RuntimeError(
            "Expected exactly psd_coffee.csv and psd.csv in MinIO; "
            f"found names={sorted(csv_names)}, count={len(psd_csv_keys)}"
        )

    xls_names = {PurePosixPath(key).name for key in psd_xls_keys}
    if xls_names != EXPECTED_LEGACY_XLS_OBJECTS or len(psd_xls_keys) != 1:
        raise RuntimeError(
            "Expected the retained legacy psd.xls object (excluded from "
            f"ingestion); found names={sorted(xls_names)}, count={len(psd_xls_keys)}"
        )

    print("STATISTICS_DOCX_OBJECTS:", len(docx_keys), flush=True)
    print("PSD_CSV_OBJECTS_SELECTED:", len(psd_csv_keys), flush=True)
    print("LEGACY_XLS_OBJECTS_IGNORED:", len(psd_xls_keys), flush=True)

    return sorted(docx_keys), sorted(psd_csv_keys)


def extract_domain_records(minio: MinioDirect, keys: list[str], domain: str):
    rows = []
    object_summaries = []

    for object_index, key in enumerate(keys, start=1):
        raw = minio.read(key)
        digest = verify_metadata(minio, key, raw)
        filename = PurePosixPath(key).name
        suffix = PurePosixPath(key).suffix.lower()
        source = source_from_key(key)

        if domain == "coffee_statistics" and suffix == ".docx":
            record_type = "official_statistics_docx_table_row"
            parsed_rows = parse_docx_rows(key, raw)
        elif domain == "coffee_psd" and suffix == ".csv":
            if filename.lower() == "psd_coffee.csv":
                record_type = "usda_psd_long_csv_row"
            elif filename.lower() == "psd.csv":
                record_type = "usda_psd_wide_csv_row"
            else:
                raise RuntimeError(f"Unrecognized PSD CSV filename: {filename}")
            parsed_rows = parse_csv_rows(key, raw)
        else:
            raise RuntimeError(f"Unsupported source format: {key}")

        for position, parsed in enumerate(parsed_rows, start=1):
            payload = {
                "source_filename": filename,
                "record_type": record_type,
                "record_position": position,
                "extracted_record": parsed,
            }
            rows.append((
                record_id_for(key, position),
                key,
                position,
                domain,
                source,
                record_type,
                digest,
                json_record(payload),
            ))

        object_summaries.append({
            "filename": filename,
            "object_key": key,
            "size_bytes": len(raw),
            "sha256": digest,
            "record_type": record_type,
            "records": len(parsed_rows),
            "columns": (
                len(parsed_rows[0].get("columns", []))
                if parsed_rows and "columns" in parsed_rows[0]
                else None
            ),
        })

        print(
            f"{domain.upper()}_OBJECT: {object_index}/{len(keys)} "
            f"file={filename} records={len(parsed_rows)} "
            f"type={record_type}",
            flush=True,
        )

    if not rows:
        raise RuntimeError(f"No records extracted for domain {domain}")

    record_ids = [row[0] for row in rows]
    if len(record_ids) != len(set(record_ids)):
        raise RuntimeError(f"Duplicate record IDs in {domain}")

    return rows, object_summaries


def distributed_dataframe(spark, rows: list[tuple]):
    # Avoid sending the entire 87k-row local list as one oversized Spark task.
    # Keep roughly <= 1,000 records per input partition, with a safe cap.
    partitions = max(4, min(128, (len(rows) + 999) // 1000))
    rdd = spark.sparkContext.parallelize(rows, numSlices=partitions)
    print("SPARK_INPUT_PARTITIONS:", partitions, flush=True)
    return spark.createDataFrame(rdd, schema=SCHEMA)


def validate_spark_input(spark, rows: list[tuple], domain: str):
    df = distributed_dataframe(spark, rows).cache()
    try:
        total = df.count()
        unique = df.select("record_id").distinct().count()
        nulls = df.filter(
            "record_id IS NULL OR object_key IS NULL OR "
            "record_position IS NULL OR raw_record_json IS NULL"
        ).count()

        print(f"{domain.upper()}_SPARK_ROWS:", total, flush=True)
        print(f"{domain.upper()}_UNIQUE_RECORD_IDS:", unique, flush=True)
        print(f"{domain.upper()}_REQUIRED_NULL_ROWS:", nulls, flush=True)

        if total != len(rows) or unique != total or nulls:
            raise RuntimeError(f"Spark validation failed for {domain}")

        df.groupBy("record_type").count().orderBy("record_type").show(
            truncate=False
        )
        df.select(
            "object_key", "record_position", "record_type", "raw_record_json"
        ).orderBy("object_key", "record_position").show(
            3, truncate=220, vertical=True
        )
    finally:
        df.unpersist()



def merge_bronze_distributed(
    spark,
    table: str,
    rows: list[tuple],
    temp_view: str,
    label: str,
):
    from pyspark.sql.functions import col

    if not rows:
        raise RuntimeError("No data records found; refusing empty ingestion")

    df = distributed_dataframe(spark, rows).cache()
    try:
        total = df.count()
        unique = df.select("record_id").distinct().count()
        if unique != total:
            raise RuntimeError(
                f"Duplicate record IDs in {table}: total={total}, unique={unique}"
            )

        required_nulls = col(SCHEMA.names[0]).isNull()
        for name in SCHEMA.names[1:]:
            required_nulls = required_nulls | col(name).isNull()
        if df.filter(required_nulls).limit(1).count():
            raise RuntimeError(f"Null required fields in Bronze input for {table}")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        sql_types = {"string": "STRING", "bigint": "BIGINT"}
        definitions = ", ".join(
            f"{field.name} {sql_types[field.dataType.simpleString()]}"
            for field in SCHEMA.fields
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ({definitions}) USING iceberg"
        )

        target = spark.table(table)
        if set(target.columns) != set(SCHEMA.names):
            raise RuntimeError(
                f"Existing table schema differs: {table}. No changes were written."
            )

        comparison = df.alias("incoming").join(
            target.alias("stored"), on="record_id", how="inner"
        )
        mismatch = None
        for name in SCHEMA.names:
            if name == "record_id":
                continue
            term = ~col(f"incoming.{name}").eqNullSafe(
                col(f"stored.{name}")
            )
            mismatch = term if mismatch is None else (mismatch | term)

        if comparison.filter(mismatch).limit(1).count():
            raise RuntimeError(
                f"Existing {table} row differs from current MinIO raw; "
                "stop and investigate"
            )

        new_count = (
            df.select("record_id")
            .join(target.select("record_id"), on="record_id", how="left_anti")
            .count()
        )
        print(f"{label}_NEW_RECORDS:", new_count, flush=True)

        if new_count:
            df.createOrReplaceTempView(temp_view)
            column_list = ", ".join(SCHEMA.names)
            source_list = ", ".join(
                f"source.{name}" for name in SCHEMA.names
            )
            spark.sql(f"""
                MERGE INTO {table} AS target
                USING {temp_view} AS source
                ON target.record_id = source.record_id
                WHEN NOT MATCHED THEN
                  INSERT ({column_list}) VALUES ({source_list})
            """)

        final_table = spark.table(table)
        final_count = final_table.count()
        matched = final_table.join(
            df.select("record_id"), on="record_id", how="inner"
        ).count()

        print(f"{label}_TOTAL:", final_count, flush=True)
        print(f"{label}_SOURCE_RECORDS_PRESENT:", matched, flush=True)

        if matched != total:
            raise RuntimeError(
                f"Only {matched}/{total} source records present in {table}"
            )

        print(f"{label}_PASSED", flush=True)
    finally:
        df.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write validated records into Bronze Iceberg. Default is dry-run.",
    )
    args = parser.parse_args()

    minio = MinioDirect()
    spark = spark_session("TLCN-Bronze-Coffee-New-Sources")

    try:
        docx_keys, psd_csv_keys = selected_source_objects(minio)

        statistics_rows, statistics_summary = extract_domain_records(
            minio, docx_keys, "coffee_statistics"
        )
        psd_rows, psd_summary = extract_domain_records(
            minio, psd_csv_keys, "coffee_psd"
        )

        print("\n===== SOURCE PROFILE =====", flush=True)
        print("SELECTED_SOURCE_OBJECTS:", len(docx_keys) + len(psd_csv_keys), flush=True)
        print("IGNORED_LEGACY_XLS:", 1, flush=True)
        print("STATISTICS_SOURCE_RECORDS:", len(statistics_rows), flush=True)
        print("PSD_SOURCE_RECORDS:", len(psd_rows), flush=True)
        print("TOTAL_SOURCE_RECORDS:", len(statistics_rows) + len(psd_rows), flush=True)

        for summary in statistics_summary + psd_summary:
            print(
                "OBJECT_SUMMARY:",
                json.dumps(summary, ensure_ascii=False),
                flush=True,
            )

        validate_spark_input(spark, statistics_rows, "coffee_statistics")
        validate_spark_input(spark, psd_rows, "coffee_psd")

        print("BRONZE_COFFEE_NEW_DRY_RUN_PASSED", flush=True)

        if not args.write:
            print("NO_ICEBERG_TABLES_WRITTEN", flush=True)
            return

        merge_bronze_distributed(
            spark,
            STATISTICS_TABLE,
            statistics_rows,
            "coffee_statistics_source_incoming",
            "BRONZE_COFFEE_STATISTICS",
        )
        merge_bronze_distributed(
            spark,
            PSD_TABLE,
            psd_rows,
            "coffee_psd_source_incoming",
            "BRONZE_COFFEE_PSD",
        )

        print(
            "BRONZE_COFFEE_STATISTICS_ICEBERG_TOTAL:",
            spark.table(STATISTICS_TABLE).count(),
            flush=True,
        )
        print(
            "BRONZE_COFFEE_PSD_ICEBERG_TOTAL:",
            spark.table(PSD_TABLE).count(),
            flush=True,
        )
        print("BRONZE_COFFEE_NEW_ICEBERG_PASSED", flush=True)
        print("SPARK_JOB_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
