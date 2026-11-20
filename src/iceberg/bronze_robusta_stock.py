
"""Load Robusta Stock CSV objects from MinIO into Iceberg Bronze.

MinIO -> Spark Driver -> Iceberg Bronze
No local CSV staging.
"""

from collections import Counter
import hashlib
import json

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

from minio_direct import (
    MinioDirect,
    csv_records,
    json_record,
    merge_into_bronze,
    record_id_for,
    spark_session,
)

TABLE = "lakehouse.bronze.robusta_stock_raw"

PREFIX = (
    "bronze/robusta_stock/"
    "source=robusta_stock_report/"
)

EXPECTED_OBJECTS = 252

EXPECTED_COLUMNS = {
    "Commodity",
    "CutOffDate",
    "PortId",
    "LotsWithValCert",
    "LotsNonTend",
    "LotsSuspended",
}

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("record_position", LongType(), False),
    StructField("batch_id", StringType(), False),
    StructField("ingestion_date", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("source", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("object_sha256", StringType(), False),
    StructField("raw_record_json", StringType(), False),
])


def main():

    minio = MinioDirect()

    all_keys = sorted(minio.list_keys(PREFIX))

    raw_keys = [
        key for key in all_keys
        if "/raw/" in key and key.lower().endswith(".csv")
    ]

    metadata_keys = {
        key for key in all_keys
        if key.endswith("/metadata.json")
    }

    print("===== ROBUSTA STOCK SOURCE =====", flush=True)
    print("TOTAL OBJECTS:", len(all_keys), flush=True)
    print("RAW CSV:", len(raw_keys), flush=True)
    print("METADATA:", len(metadata_keys), flush=True)

    if len(all_keys) != EXPECTED_OBJECTS * 2:
        raise RuntimeError("Unexpected MinIO object count")

    if len(raw_keys) != EXPECTED_OBJECTS:
        raise RuntimeError("Expected exactly 252 raw CSV files")

    if len(metadata_keys) != EXPECTED_OBJECTS:
        raise RuntimeError("Expected exactly 252 metadata files")

    rows = []
    counts = Counter()

    for file_index, key in enumerate(raw_keys, 1):

        filename = key.rsplit("/", 1)[-1]
        metadata_key = key.split("/raw/")[0] + "/metadata.json"

        if metadata_key not in metadata_keys:
            raise RuntimeError(f"Missing metadata: {filename}")

        raw = minio.read(key)

        metadata = json.loads(
            minio.read(metadata_key).decode("utf-8-sig")
        )

        digest = hashlib.sha256(raw).hexdigest()

        # Verify raw data against its metadata.
        if metadata.get("raw_object_key") != key:
            raise RuntimeError(f"Object key mismatch: {filename}")

        if metadata.get("sha256") != digest:
            raise RuntimeError(f"SHA256 mismatch: {filename}")

        if metadata.get("size_bytes") != len(raw):
            raise RuntimeError(f"File size mismatch: {filename}")

        if metadata.get("status") != "success":
            raise RuntimeError(f"Invalid metadata status: {filename}")

        batch_id = metadata.get("batch_id")
        ingestion_date = metadata.get("ingestion_date")
        domain = metadata.get("dataset_domain")
        source = metadata.get("source_name")

        if not all([batch_id, ingestion_date, domain, source]):
            raise RuntimeError(f"Missing lineage metadata: {filename}")

        grand_total_count = 0
        port_ids = set()
        file_rows = 0

        for position, record in enumerate(
            csv_records(key, raw), 1
        ):

            if set(record.keys()) != EXPECTED_COLUMNS:
                raise ValueError(
                    f"Unexpected CSV columns: {filename}"
                )

            commodity = (record.get("Commodity") or "").strip()
            port_id = (record.get("PortId") or "").strip()

            if commodity == "GrandTotal":
                record_type = "grand_total"
                grand_total_count += 1

            elif commodity == "RC":

                record_type = "port_stock"

                if not port_id:
                    raise ValueError(
                        f"Missing PortId: {filename}, row={position}"
                    )

                if not record.get("CutOffDate"):
                    raise ValueError(
                        f"Missing CutOffDate: {filename}, row={position}"
                    )

                if port_id in port_ids:
                    raise ValueError(
                        f"Duplicate port {port_id}: {filename}"
                    )

                port_ids.add(port_id)

            else:
                raise ValueError(
                    f"Unexpected Commodity={commodity}: {filename}"
                )

            rows.append((
                record_id_for(key, position),
                key,
                position,
                batch_id,
                ingestion_date,
                domain,
                source,
                filename,
                record_type,
                digest,
                json_record(record),
            ))

            counts[record_type] += 1
            file_rows += 1

        if grand_total_count != 1:
            raise RuntimeError(
                f"{filename}: expected one GrandTotal, "
                f"found {grand_total_count}"
            )

        if file_rows == 0:
            raise RuntimeError(f"Empty CSV: {filename}")

        print(
            f"SOURCE_FILE: {file_index}/252 "
            f"{filename} rows={file_rows}",
            flush=True,
        )

    print("\n===== SOURCE PROFILE =====", flush=True)
    print("SOURCE_FILES:", len(raw_keys), flush=True)
    print("SOURCE_RECORDS:", len(rows), flush=True)
    print("PORT_STOCK:", counts["port_stock"], flush=True)
    print("GRAND_TOTAL:", counts["grand_total"], flush=True)

    if counts["grand_total"] != EXPECTED_OBJECTS:
        raise RuntimeError("GrandTotal count differs from file count")

    spark = spark_session("TLCN-Bronze-Robusta-Stock")

    try:

        merge_into_bronze(
            spark,
            TABLE,
            SCHEMA,
            rows,
            "robusta_stock_incoming",
            "BRONZE_ROBUSTA_STOCK",
        )

        total = spark.table(TABLE).count()

        if total != len(rows):
            raise RuntimeError(
                f"Iceberg count mismatch: {total} != {len(rows)}"
            )

        spark.sql(f"""
            SELECT
                record_type,
                COUNT(*) AS records,
                COUNT(DISTINCT object_key) AS source_files
            FROM {TABLE}
            GROUP BY record_type
            ORDER BY record_type
        """).show(truncate=False)

        spark.sql(f"""
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT object_key) AS total_files,
                COUNT(DISTINCT record_id) AS unique_records
            FROM {TABLE}
        """).show(truncate=False)

        print(
            "BRONZE_ROBUSTA_STOCK_PASSED",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
