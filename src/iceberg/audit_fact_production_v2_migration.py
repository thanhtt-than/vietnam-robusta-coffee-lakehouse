"""Read-only pre-migration audit for FACT_PRODUCTION v2.

This script DOES NOT write, delete, alter, rename, or replace any Iceberg table.
It compares the current 19-row fact_production with the 19 Legacy rows embedded
in fact_production_v2_staging, and records the current Iceberg snapshot metadata.

Run:
  python -m py_compile src/iceberg/audit_fact_production_v2_migration.py
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\run_spark.ps1" -Script "src/iceberg/audit_fact_production_v2_migration.py"
"""

from pyspark.sql import SparkSession, functions as F

OLD_FACT = "lakehouse.gold.fact_production"
STAGING = "lakehouse.gold.fact_production_v2_staging"
DIM_YEAR = "lakehouse.gold.dim_year"
DIM_GEOGRAPHY = "lakehouse.gold.dim_geography"
DIM_SOURCE = "lakehouse.gold.dim_source"
DIM_PRODUCT = "lakehouse.gold.dim_coffee_product"

EXPECTED_STAGING_ROWS = 141
EXPECTED_LEGACY_ROWS = 19
EXPECTED_DATASETS = {
    "LEGACY_VIETNAM_ANNUAL": 19,
    "PROVINCE_STATISTICS": 65,
    "REGION_STATISTICS": 30,
    "VIETNAM_ANNUAL_STATISTICS": 11,
    "WORLD_MAJOR_COUNTRIES_2024": 16,
}

# Compare every business/lineage field shared by the v1 Fact and v2 Legacy rows.
COMPARE_COLUMNS = [
    "geography_key",
    "area_source_key",
    "production_source_key",
    "coffee_area_thousand_ha",
    "coffee_production_thousand_tons",
    "area_growth_index_prev_year_100",
    "production_growth_index_prev_year_100",
    "area_publication_status",
    "production_publication_status",
    "area_quality_status",
    "production_quality_status",
    "area_raw_observation_count",
    "production_raw_observation_count",
    "area_bronze_record_id",
    "production_bronze_record_id",
    "area_bronze_object_key",
    "production_bronze_object_key",
]


def make_spark():
    return (
        SparkSession.builder
        .appName("TLCN-Audit-Fact-Production-V2-Migration")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def require_columns(df, required, label):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise RuntimeError(f"{label} missing columns: {missing}")


def count_unmatched(left, right, key, label):
    n = (
        left.select(key).distinct()
        .join(right.select(key).distinct(), key, "left_anti")
        .count()
    )
    print(f"{label}: {n}", flush=True)
    return n


def print_snapshot_history(spark):
    print("===== OLD FACT SNAPSHOT HISTORY (READ ONLY) =====", flush=True)
    try:
        snapshots = spark.table(OLD_FACT + ".snapshots")
        cols = [c for c in ["committed_at", "snapshot_id", "parent_id", "operation", "summary"] if c in snapshots.columns]
        snapshots.select(*cols).orderBy(F.col("committed_at").desc()).show(10, truncate=False)
        latest = snapshots.orderBy(F.col("committed_at").desc()).select("snapshot_id", "committed_at").first()
        if latest:
            print("OLD_FACT_LATEST_SNAPSHOT_ID:", latest["snapshot_id"], flush=True)
            print("OLD_FACT_LATEST_SNAPSHOT_COMMITTED_AT:", latest["committed_at"], flush=True)
    except Exception as exc:
        print("OLD_FACT_SNAPSHOT_HISTORY_UNAVAILABLE:", str(exc)[:1000], flush=True)
        print("This does not modify the table; continue with row-level audit.", flush=True)


def main():
    spark = make_spark()
    try:
        old = spark.table(OLD_FACT)
        stage = spark.table(STAGING)

        require_columns(old, ["production_fact_key", "year_key"] + COMPARE_COLUMNS, "OLD_FACT")
        require_columns(
            stage,
            [
                "production_fact_key", "year_key", "geography_key",
                "coffee_product_key", "dataset_code", "area_measure_type",
                "is_preliminary", "publication_status_code",
            ] + COMPARE_COLUMNS,
            "V2_STAGING",
        )

        print("===== TABLE COUNTS =====", flush=True)
        old_count = old.count()
        stage_count = stage.count()
        legacy = stage.filter(F.col("dataset_code") == "LEGACY_VIETNAM_ANNUAL")
        legacy_count = legacy.count()

        print("FACT_PRODUCTION_V1_ROWS:", old_count, flush=True)
        print("FACT_PRODUCTION_V2_STAGING_ROWS:", stage_count, flush=True)
        print("FACT_PRODUCTION_V2_LEGACY_ROWS:", legacy_count, flush=True)

        if old_count != EXPECTED_LEGACY_ROWS:
            raise RuntimeError(f"Old Fact expected {EXPECTED_LEGACY_ROWS} rows, found {old_count}")
        if stage_count != EXPECTED_STAGING_ROWS:
            raise RuntimeError(f"Staging expected {EXPECTED_STAGING_ROWS} rows, found {stage_count}")
        if legacy_count != EXPECTED_LEGACY_ROWS:
            raise RuntimeError(f"Staging Legacy expected {EXPECTED_LEGACY_ROWS} rows, found {legacy_count}")

        print("===== STAGING DATASET COUNTS =====", flush=True)
        actual_datasets = {
            r["dataset_code"]: r["count"]
            for r in stage.groupBy("dataset_code").count().collect()
        }
        print("FACT_PRODUCTION_V2_DATASET_COUNTS:", actual_datasets, flush=True)
        if actual_datasets != EXPECTED_DATASETS:
            raise RuntimeError(
                f"Dataset count mismatch. expected={EXPECTED_DATASETS}, actual={actual_datasets}"
            )

        print("===== CURRENT DIMENSION COUNTS =====", flush=True)
        dimension_expectations = [
            (DIM_YEAR, 27, "year_key"),
            (DIM_GEOGRAPHY, 33, "geography_key"),
            (DIM_SOURCE, 15, "source_key"),
            (DIM_PRODUCT, 1, "coffee_product_key"),
        ]
        for table, expected, key in dimension_expectations:
            df = spark.table(table)
            rows = df.count()
            unique = df.select(key).distinct().count()
            nulls = df.filter(F.col(key).isNull()).count()
            print(
                "DIMENSION_AUDIT:", table,
                "ROWS:", rows,
                "UNIQUE_PK:", unique,
                "NULL_PK:", nulls,
                flush=True,
            )
            if rows != expected or unique != rows or nulls:
                raise RuntimeError(
                    f"Dimension audit failed for {table}: rows={rows}, unique={unique}, nulls={nulls}"
                )

        print("===== LEGACY YEAR COVERAGE =====", flush=True)
        old_years = old.select("year_key").distinct()
        legacy_years = legacy.select("year_key").distinct()
        missing_from_stage = count_unmatched(
            old_years, legacy_years, "year_key", "OLD_YEARS_MISSING_FROM_V2_LEGACY"
        )
        extra_in_stage = count_unmatched(
            legacy_years, old_years, "year_key", "V2_LEGACY_YEARS_NOT_IN_OLD_FACT"
        )
        if missing_from_stage or extra_in_stage:
            raise RuntimeError("Legacy year coverage differs between v1 and v2.")

        print("===== LEGACY BUSINESS VALUE AND LINEAGE COMPARISON =====", flush=True)
        print("OLD_FACT_SCHEMA:", old.schema.simpleString(), flush=True)
        print("V2_LEGACY_SCHEMA:", legacy.schema.simpleString(), flush=True)
        old_alias = old.select(
            F.col("year_key").cast("int").alias("_year_key"),
            *[F.col(c).alias("old_" + c) for c in COMPARE_COLUMNS],
        ).alias("o")
        new_alias = legacy.select(
            F.col("year_key").cast("int").alias("_year_key"),
            *[F.col(c).alias("new_" + c) for c in COMPARE_COLUMNS],
        ).alias("n")

        joined = old_alias.join(new_alias, "_year_key", "full")
        # Year coverage was checked above with left-anti joins. The full join
        # below compares all shared business and lineage fields by year.

        mismatch_conditions = []
        for c in COMPARE_COLUMNS:
            mismatch_conditions.append(~F.col("old_" + c).eqNullSafe(F.col("new_" + c)))
        mismatch_condition = mismatch_conditions[0]
        for condition in mismatch_conditions[1:]:
            mismatch_condition = mismatch_condition | condition

        mismatches = joined.filter(mismatch_condition)
        mismatch_count = mismatches.count()
        print("LEGACY_ROW_MISMATCHES:", mismatch_count, flush=True)
        if mismatch_count:
            print("LEGACY_MISMATCH_SAMPLE:", flush=True)
            mismatches.select(
                "_year_key",
                *[
                    x for c in COMPARE_COLUMNS
                    for x in ["old_" + c, "new_" + c]
                ],
            ).show(30, truncate=False)

        # Ensure v2 legacy rows retain the national Vietnam/product-unspecified grain.
        wrong_legacy_grain = legacy.filter(
            (F.col("geography_key") != 1)
            | (F.col("coffee_product_key") != 1)
            | (F.col("area_measure_type") != "REPORTED_AREA")
        ).count()
        print("LEGACY_WRONG_GEOGRAPHY_PRODUCT_OR_AREA_TYPE_ROWS:", wrong_legacy_grain, flush=True)

        old_2006 = old.filter(F.col("year_key") == 2006).count()
        new_2006 = legacy.filter(F.col("year_key") == 2006).count()
        print("OLD_FACT_2006_ROWS:", old_2006, flush=True)
        print("V2_LEGACY_2006_ROWS:", new_2006, flush=True)

        if mismatch_count or wrong_legacy_grain or old_2006 or new_2006:
            raise RuntimeError("Legacy preservation audit failed. Do not migrate.")

        print_snapshot_history(spark)

        print("FACT_PRODUCTION_V2_MIGRATION_READ_ONLY_AUDIT_PASSED", flush=True)
        print("NO_TABLE_WRITTEN_OR_MODIFIED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
