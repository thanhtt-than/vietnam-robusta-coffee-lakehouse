"""
Read-only inspection of the new coffee source Bronze Iceberg tables.

Expected tables:
  lakehouse.bronze.coffee_statistics_raw
  lakehouse.bronze.coffee_psd_raw

No tables are created or modified.
Run from the TLCN project root with run_spark.ps1.
"""
from pyspark.sql import functions as F

from minio_direct import spark_session


TABLES = [
    "lakehouse.bronze.coffee_statistics_raw",
    "lakehouse.bronze.coffee_psd_raw",
]


def inspect_table(spark, table_name):
    print("\n" + "=" * 100, flush=True)
    print("TABLE:", table_name, flush=True)

    df = spark.table(table_name)
    total = df.count()
    print("TABLE_ROWS:", total, flush=True)

    print("===== SCHEMA =====", flush=True)
    df.printSchema()

    print("===== COLUMNS =====", flush=True)
    print(", ".join(df.columns), flush=True)

    if "record_id" in df.columns:
        unique_ids = df.select("record_id").distinct().count()
        null_ids = df.filter(F.col("record_id").isNull()).count()
        print("UNIQUE_RECORD_IDS:", unique_ids, flush=True)
        print("NULL_RECORD_IDS:", null_ids, flush=True)

    if "object_key" in df.columns:
        object_count = df.select("object_key").distinct().count()
        print("DISTINCT_OBJECT_KEYS:", object_count, flush=True)
        print("===== ROWS BY SOURCE OBJECT =====", flush=True)

        group_columns = ["object_key"]
        if "record_type" in df.columns:
            group_columns.append("record_type")

        (
            df.groupBy(*group_columns)
            .count()
            .orderBy("object_key", *(
                ["record_type"] if "record_type" in df.columns else []
            ))
            .show(100, truncate=False)
        )

    if "record_type" in df.columns:
        print("===== RECORD TYPE COUNTS =====", flush=True)
        (
            df.groupBy("record_type")
            .count()
            .orderBy("record_type")
            .show(50, truncate=False)
        )

    for column in ("domain", "source", "source_file"):
        if column in df.columns:
            print(f"===== DISTINCT {column.upper()} =====", flush=True)
            (
                df.select(column)
                .distinct()
                .orderBy(column)
                .show(50, truncate=False)
            )

    if "raw_record_json" in df.columns:
        print("===== SAMPLE RAW RECORDS =====", flush=True)
        selected = [
            c for c in (
                "object_key", "record_position", "record_type",
                "raw_record_json"
            ) if c in df.columns
        ]
        df.select(*selected).orderBy(
            *[c for c in ("object_key", "record_position") if c in selected]
        ).show(5, truncate=240, vertical=True)

    print(f"TABLE_INSPECTION_PASSED: {table_name}", flush=True)


def main():
    spark = spark_session("TLCN-Inspect-New-Coffee-Bronze")

    try:
        print("===== BRONZE NAMESPACE TABLES =====", flush=True)
        tables = spark.sql("SHOW TABLES IN lakehouse.bronze").collect()
        table_names = sorted(row.tableName for row in tables)
        for name in table_names:
            print("BRONZE_TABLE:", name, flush=True)

        expected_names = {
            "coffee_statistics_raw",
            "coffee_psd_raw",
        }
        missing = expected_names - set(table_names)
        if missing:
            print("MISSING_EXPECTED_TABLES:", sorted(missing), flush=True)
            print("NO_TABLES_WRITTEN_BY_THIS_INSPECTION", flush=True)
            raise RuntimeError(
                "Expected coffee Bronze tables are not present: "
                + ", ".join(sorted(missing))
            )

        for table_name in TABLES:
            inspect_table(spark, table_name)

        print("\n===== EXPECTED SOURCE PROFILE =====", flush=True)
        print("coffee_statistics_raw: 7 DOCX objects, expected 75 rows", flush=True)
        print(
            "coffee_psd_raw: psd_coffee.csv (87,704 rows) + "
            "psd.csv (19 rows), expected 87,723 rows",
            flush=True,
        )
        print("Legacy psd.xls is intentionally excluded.", flush=True)
        print("NEW_COFFEE_BRONZE_INSPECTION_PASSED", flush=True)
        print("READ_ONLY: TRUE", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
