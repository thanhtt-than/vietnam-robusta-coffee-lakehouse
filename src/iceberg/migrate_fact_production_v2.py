"""Safely promote the validated V2 Production Fact to the official Iceberg table.

This script is intentionally split into two modes:

1) Default mode: READ-ONLY dry run. It validates the current V1 Fact, V2
   staging Fact, dimensions, row counts, grain, keys, and V1-to-V2 legacy parity.
2) --execute: creates and verifies a separate V1 backup, then atomically replaces
   lakehouse.gold.fact_production with the validated V2 staging table.

Safety guarantees:
- The V1 backup is never overwritten.
- The V2 staging table is retained after promotion.
- No DROP, PURGE, DELETE, or table rename is used by this migration script.
- Promotion is refused if any preflight validation fails.

Run from the project root in PowerShell:
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\\run_spark.ps1" -Script "src/iceberg/migrate_fact_production_v2.py"

After reviewing a successful dry run, execute the migration:
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\\run_spark.ps1" -Script "src/iceberg/migrate_fact_production_v2.py" -ScriptArgs "--execute"

Save this file as src/iceberg/migrate_fact_production_v2.py in the project.
"""

from __future__ import annotations

import argparse
from functools import reduce

from pyspark.sql import SparkSession, functions as F


# Iceberg tables
OLD_FACT = "lakehouse.gold.fact_production"
STAGING = "lakehouse.gold.fact_production_v2_staging"
V1_BACKUP = "lakehouse.gold.fact_production_v1_backup"

# Expected source-controlled contract from gold_fact_production_v2_staging.py
EXPECTED_V1_ROWS = 19
EXPECTED_V2_ROWS = 141
EXPECTED_STATISTICS_MISSING_VALUES = 4
EXPECTED_DATASETS = {
    "LEGACY_VIETNAM_ANNUAL": 19,
    "PROVINCE_STATISTICS": 65,
    "REGION_STATISTICS": 30,
    "VIETNAM_ANNUAL_STATISTICS": 11,
    "WORLD_MAJOR_COUNTRIES_2024": 16,
}

V1_COLUMNS = [
    "production_fact_key",
    "year_key",
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
    "gold_processed_at",
]

V2_COLUMNS = [
    "production_fact_key",
    "year_key",
    "geography_key",
    "coffee_product_key",
    "dataset_code",
    "area_measure_type",
    "is_preliminary",
    "publication_status_code",
    "coffee_area_thousand_ha",
    "coffee_production_thousand_tons",
    "yield_tons_per_ha",
    "area_growth_index_prev_year_100",
    "production_growth_index_prev_year_100",
    "area_source_key",
    "production_source_key",
    "yield_source_key",
    "area_publication_status",
    "production_publication_status",
    "yield_publication_status",
    "area_quality_status",
    "production_quality_status",
    "yield_quality_status",
    "area_raw_observation_count",
    "production_raw_observation_count",
    "yield_raw_observation_count",
    "area_silver_record_id",
    "production_silver_record_id",
    "yield_silver_record_id",
    "area_bronze_record_id",
    "production_bronze_record_id",
    "yield_bronze_record_id",
    "area_bronze_object_key",
    "production_bronze_object_key",
    "yield_bronze_object_key",
    "area_bronze_record_position",
    "production_bronze_record_position",
    "yield_bronze_record_position",
    "area_object_sha256",
    "production_object_sha256",
    "yield_object_sha256",
    "area_source_filename",
    "production_source_filename",
    "yield_source_filename",
    "area_source_table",
    "production_source_table",
    "yield_source_table",
    "yield_method",
    "gold_processed_at",
]

# Natural grain defined in gold_fact_production_v2_staging.py
GRAIN_KEYS = [
    "year_key",
    "geography_key",
    "coffee_product_key",
    "dataset_code",
    "area_measure_type",
    "publication_status_code",
]

# V1 fields that must remain identical to the V2 LEGACY_VIETNAM_ANNUAL rows.
# The surrogate PK and processing timestamp are intentionally not compared:
# V2 uses a composite-grain hash key and a new processing timestamp.
V1_TO_V2_COMPARE_COLUMNS = [
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

# When a V2 table is already promoted, ignore only the processing timestamp
# when comparing it with staging. The data and lineage must still match.
V2_DATA_COLUMNS = [c for c in V2_COLUMNS if c != "gold_processed_at"]

QUALITY_STATUSES = ("PASS", "MISSING_SOURCE_VALUE", "NOT_PROVIDED")

DIMENSION_SPECS = [
    ("year_key", "lakehouse.gold.dim_year", "year_key", False),
    ("geography_key", "lakehouse.gold.dim_geography", "geography_key", False),
    (
        "coffee_product_key",
        "lakehouse.gold.dim_coffee_product",
        "coffee_product_key",
        False,
    ),
    ("area_source_key", "lakehouse.gold.dim_source", "source_key", True),
    ("production_source_key", "lakehouse.gold.dim_source", "source_key", True),
    ("yield_source_key", "lakehouse.gold.dim_source", "source_key", True),
]


def make_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TLCN-Migrate-Fact-Production-V2-Local-Validation")
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
    spark.sparkContext.setLogLevel("WARN")
    return spark


def require_table(spark: SparkSession, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required Iceberg table does not exist: {table_name}")


def assert_unique(df, columns: list[str], label: str) -> None:
    total = df.count()
    unique = df.select(*columns).distinct().count()
    if total != unique:
        duplicates = (
            df.groupBy(*columns)
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
        )
        print(f"{label}_DUPLICATE_SAMPLE:", flush=True)
        duplicates.show(20, truncate=False)
        raise RuntimeError(
            f"{label}: duplicate key {columns}; duplicate_rows={total - unique}"
        )


def assert_same_rows(left, right, label: str) -> None:
    if left.columns != right.columns:
        raise RuntimeError(
            f"{label}: column order/schema mismatch; "
            f"left={left.columns}; right={right.columns}"
        )
    left_only = left.exceptAll(right).limit(1).count()
    right_only = right.exceptAll(left).limit(1).count()
    if left_only or right_only:
        raise RuntimeError(
            f"{label}: row content mismatch; "
            f"left_only={left_only}; right_only={right_only}"
        )


def validate_v2_schema(stage) -> None:
    if stage.columns != V2_COLUMNS:
        raise RuntimeError(
            "V2 staging schema does not match the approved V2 contract. "
            f"actual={stage.columns}"
        )

    actual_types = {field.name: field.dataType.simpleString() for field in stage.schema}
    expected_types = {
        "production_fact_key": "bigint",
        "year_key": "int",
        "geography_key": "int",
        "coffee_product_key": "int",
        "dataset_code": "string",
        "area_measure_type": "string",
        "is_preliminary": "boolean",
        "publication_status_code": "string",
        "coffee_area_thousand_ha": "decimal(18,3)",
        "coffee_production_thousand_tons": "decimal(18,3)",
        "yield_tons_per_ha": "decimal(18,3)",
        "area_source_key": "bigint",
        "production_source_key": "bigint",
        "yield_source_key": "bigint",
    }
    mismatches = {
        column: (expected, actual_types.get(column))
        for column, expected in expected_types.items()
        if actual_types.get(column) != expected
    }
    if mismatches:
        raise RuntimeError(f"V2 staging data type mismatch: {mismatches}")


def validate_v2_dimensions(spark: SparkSession, stage) -> None:
    for fact_key, dimension_table, dimension_key, nullable in DIMENSION_SPECS:
        fact_keys = stage.select(F.col(fact_key).alias("_fact_key")).distinct()
        if nullable:
            fact_keys = fact_keys.filter(F.col("_fact_key").isNotNull())

        dimension_keys = (
            spark.table(dimension_table)
            .select(F.col(dimension_key).alias("_dimension_key"))
            .distinct()
        )
        unmatched = fact_keys.join(
            dimension_keys,
            fact_keys["_fact_key"] == dimension_keys["_dimension_key"],
            "left_anti",
        ).count()
        print(f"FACT_PRODUCTION_V2_UNMATCHED_{fact_key.upper()}:", unmatched, flush=True)
        if unmatched:
            raise RuntimeError(
                f"V2 contains {unmatched} unmatched values for {fact_key}"
            )

    # A source key is required when a measure is supplied by a source.
    for prefix in ("area", "production", "yield"):
        status_col = f"{prefix}_quality_status"
        source_col = f"{prefix}_source_key"
        missing_source = stage.filter(
            (F.col(status_col) != "NOT_PROVIDED") & F.col(source_col).isNull()
        ).count()
        unexpected_source = stage.filter(
            (F.col(status_col) == "NOT_PROVIDED") & F.col(source_col).isNotNull()
        ).count()
        if missing_source or unexpected_source:
            raise RuntimeError(
                f"V2 {prefix} source-key/status mismatch: "
                f"missing_source={missing_source}; unexpected_source={unexpected_source}"
            )


def validate_v2_quality(stage) -> None:
    for prefix, value_col in (
        ("area", "coffee_area_thousand_ha"),
        ("production", "coffee_production_thousand_tons"),
        ("yield", "yield_tons_per_ha"),
    ):
        status_col = f"{prefix}_quality_status"
        invalid_status = stage.filter(
            F.col(status_col).isNull() | (~F.col(status_col).isin(*QUALITY_STATUSES))
        ).count()
        if invalid_status:
            raise RuntimeError(f"V2 has {invalid_status} invalid values in {status_col}")

        inconsistent = stage.filter(
            ((F.col(status_col) == "PASS") & F.col(value_col).isNull())
            | (
                F.col(status_col).isin("MISSING_SOURCE_VALUE", "NOT_PROVIDED")
                & F.col(value_col).isNotNull()
            )
        ).count()
        if inconsistent:
            raise RuntimeError(
                f"V2 has {inconsistent} quality/value inconsistencies for {prefix}"
            )

    missing_measure_count = sum(
        stage.filter(F.col(f"{prefix}_quality_status") == "MISSING_SOURCE_VALUE").count()
        for prefix in ("area", "production", "yield")
    )
    if missing_measure_count != EXPECTED_STATISTICS_MISSING_VALUES:
        raise RuntimeError(
            "Unexpected number of missing source measure values: "
            f"expected={EXPECTED_STATISTICS_MISSING_VALUES}; actual={missing_measure_count}"
        )

    negative = stage.filter(
        (F.col("coffee_area_thousand_ha") < 0)
        | (F.col("coffee_production_thousand_tons") < 0)
        | (F.col("yield_tons_per_ha") < 0)
    ).count()
    if negative:
        raise RuntimeError(f"V2 has {negative} rows with negative production measures")


def validate_v2(spark: SparkSession, stage) -> None:
    validate_v2_schema(stage)

    total = stage.count()
    unique_pk = stage.select("production_fact_key").distinct().count()
    unique_grain = stage.select(*GRAIN_KEYS).distinct().count()
    print("FACT_PRODUCTION_V2_STAGING_ROWS:", total, flush=True)
    print("FACT_PRODUCTION_V2_STAGING_UNIQUE_PK:", unique_pk, flush=True)
    print("FACT_PRODUCTION_V2_STAGING_UNIQUE_GRAIN:", unique_grain, flush=True)

    if total != EXPECTED_V2_ROWS or unique_pk != total or unique_grain != total:
        raise RuntimeError(
            f"V2 row/key validation failed: rows={total}; "
            f"unique_pk={unique_pk}; unique_grain={unique_grain}"
        )

    dataset_counts = {
        row["dataset_code"]: row["count"]
        for row in stage.groupBy("dataset_code").count().collect()
    }
    print("FACT_PRODUCTION_V2_DATASET_COUNTS:", dataset_counts, flush=True)
    if dataset_counts != EXPECTED_DATASETS:
        raise RuntimeError(
            f"Unexpected V2 dataset distribution: actual={dataset_counts}; "
            f"expected={EXPECTED_DATASETS}"
        )

    required_key_nulls = stage.filter(
        F.col("production_fact_key").isNull()
        | F.col("year_key").isNull()
        | F.col("geography_key").isNull()
        | F.col("coffee_product_key").isNull()
        | F.col("dataset_code").isNull()
        | F.col("area_measure_type").isNull()
        | F.col("publication_status_code").isNull()
    ).count()
    if required_key_nulls:
        raise RuntimeError(f"V2 has {required_key_nulls} rows with NULL required keys")

    validate_v2_dimensions(spark, stage)
    validate_v2_quality(stage)
    print("FACT_PRODUCTION_V2_PROMOTION_VALIDATION_PASSED", flush=True)


def validate_v1_against_v2_legacy(spark: SparkSession, v1, stage, label: str):
    if v1.columns != V1_COLUMNS:
        raise RuntimeError(f"{label}: table does not have the approved V1 schema")

    v1_count = v1.count()
    legacy = stage.filter(F.col("dataset_code") == "LEGACY_VIETNAM_ANNUAL")
    legacy_count = legacy.count()
    print(f"{label}_ROWS:", v1_count, flush=True)
    print("V2_LEGACY_VIETNAM_ROWS:", legacy_count, flush=True)

    if v1_count != EXPECTED_V1_ROWS or legacy_count != EXPECTED_V1_ROWS:
        raise RuntimeError(
            f"{label}/V2 legacy row counts differ: v1={v1_count}; v2_legacy={legacy_count}"
        )

    assert_unique(v1, ["year_key"], f"{label}_YEAR")
    assert_unique(legacy, ["year_key"], "V2_LEGACY_YEAR")

    old_years = v1.select("year_key").distinct()
    new_years = legacy.select("year_key").distinct()
    missing_new = old_years.join(new_years, "year_key", "left_anti").count()
    extra_new = new_years.join(old_years, "year_key", "left_anti").count()
    if missing_new or extra_new:
        raise RuntimeError(
            f"{label}/V2 legacy year coverage differs: "
            f"missing={missing_new}; extra={extra_new}"
        )

    old_alias = v1.select(
        F.col("year_key").alias("_year_key"),
        *[F.col(c).alias("old_" + c) for c in V1_TO_V2_COMPARE_COLUMNS],
    )
    new_alias = legacy.select(
        F.col("year_key").alias("_year_key"),
        *[F.col(c).alias("new_" + c) for c in V1_TO_V2_COMPARE_COLUMNS],
    )
    joined = old_alias.join(new_alias, "_year_key", "inner")

    comparisons = [
        ~F.col("old_" + column).eqNullSafe(F.col("new_" + column))
        for column in V1_TO_V2_COMPARE_COLUMNS
    ]
    mismatch_condition = reduce(lambda left, right: left | right, comparisons)
    mismatch_count = joined.filter(mismatch_condition).count()
    if mismatch_count:
        raise RuntimeError(
            f"{label} differs from V2 Legacy Vietnam in {mismatch_count} rows"
        )

    # Resolve the expected dimension surrogate keys by business code; do not
    # assume that VN or COFFEE_UNSPECIFIED has key value 1.
    geo_rows = (
        spark.table("lakehouse.gold.dim_geography")
        .filter(F.col("geography_code") == "VN")
        .select("geography_key")
        .limit(2)
        .collect()
    )
    product_rows = (
        spark.table("lakehouse.gold.dim_coffee_product")
        .filter(F.col("product_code") == "COFFEE_UNSPECIFIED")
        .select("coffee_product_key")
        .limit(2)
        .collect()
    )
    if len(geo_rows) != 1 or len(product_rows) != 1:
        raise RuntimeError(
            "Cannot uniquely resolve VN geography and COFFEE_UNSPECIFIED product keys"
        )

    expected_geo_key = geo_rows[0]["geography_key"]
    expected_product_key = product_rows[0]["coffee_product_key"]
    wrong_legacy_grain = legacy.filter(
        (F.col("geography_key") != expected_geo_key)
        | (F.col("coffee_product_key") != expected_product_key)
        | (F.col("area_measure_type") != "REPORTED_AREA")
    ).count()
    if wrong_legacy_grain:
        raise RuntimeError(
            f"V2 Legacy Vietnam has {wrong_legacy_grain} unexpected grain rows"
        )

    print(f"{label}_MATCHES_V2_LEGACY: 19/19", flush=True)
    return legacy


def verify_backup_against_v1(backup, current_v1) -> None:
    if backup.columns != V1_COLUMNS:
        raise RuntimeError("V1 backup schema is not the expected V1 schema")
    if backup.count() != EXPECTED_V1_ROWS:
        raise RuntimeError("V1 backup must contain exactly 19 rows")
    assert_unique(backup, ["production_fact_key"], "V1_BACKUP_PK")
    assert_same_rows(
        current_v1.select(*V1_COLUMNS),
        backup.select(*V1_COLUMNS),
        "V1_BACKUP_VS_CURRENT_V1",
    )
    print("FACT_PRODUCTION_V1_BACKUP_VERIFIED:", EXPECTED_V1_ROWS, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate and safely promote Fact Production V2."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Create/verify the V1 backup and replace the official Fact with V2.",
    )
    args = parser.parse_args()

    spark = make_spark()
    try:
        require_table(spark, OLD_FACT)
        require_table(spark, STAGING)

        current = spark.table(OLD_FACT)
        stage = spark.table(STAGING)
        validate_v2(spark, stage)

        if current.columns == V1_COLUMNS:
            current_version = "V1"
            if current.count() != EXPECTED_V1_ROWS:
                raise RuntimeError("Current V1 Fact does not contain exactly 19 rows")
            validate_v1_against_v2_legacy(
                spark, current, stage, "CURRENT_FACT_PRODUCTION_V1"
            )
        elif current.columns == V2_COLUMNS:
            current_version = "V2"
            if current.count() != EXPECTED_V2_ROWS:
                raise RuntimeError("Current V2 Fact does not contain exactly 141 rows")
            assert_same_rows(
                current.select(*V2_DATA_COLUMNS),
                stage.select(*V2_DATA_COLUMNS),
                "CURRENT_FACT_PRODUCTION_V2_VS_STAGING",
            )
            print("CURRENT_FACT_PRODUCTION_ALREADY_MATCHES_V2_STAGING", flush=True)
        else:
            raise RuntimeError(
                "Current fact_production schema is neither approved V1 nor V2. "
                f"columns={current.columns}"
            )

        backup_exists = spark.catalog.tableExists(V1_BACKUP)
        if backup_exists:
            backup = spark.table(V1_BACKUP)
            if backup.columns != V1_COLUMNS or backup.count() != EXPECTED_V1_ROWS:
                raise RuntimeError(
                    "Existing V1 backup is not a valid 19-row V1 backup; "
                    "it will not be overwritten."
                )
            assert_unique(backup, ["production_fact_key"], "V1_BACKUP_PK")
            validate_v1_against_v2_legacy(spark, backup, stage, "V1_BACKUP")
            if current_version == "V1":
                verify_backup_against_v1(backup, current)
            print("FACT_PRODUCTION_V1_BACKUP_EXISTS_AND_IS_VALID", flush=True)
        else:
            if current_version == "V2":
                raise RuntimeError(
                    "fact_production is already V2 but fact_production_v1_backup "
                    "does not exist. Refusing to proceed without a V1 backup."
                )
            print("FACT_PRODUCTION_V1_BACKUP_WILL_BE_CREATED", flush=True)

        if not args.execute:
            print("FACT_PRODUCTION_V2_MIGRATION_DRY_RUN_PASSED", flush=True)
            print("NO_GOLD_TABLE_WRITTEN_OR_REPLACED", flush=True)
            return

        # Preserve V1 first. Never overwrite an existing backup.
        if not backup_exists:
            spark.sql(
                f"CREATE TABLE {V1_BACKUP} USING iceberg AS SELECT * FROM {OLD_FACT}"
            )
            backup = spark.table(V1_BACKUP)
            verify_backup_against_v1(backup, current)
            validate_v1_against_v2_legacy(
                spark, backup, stage, "CREATED_V1_BACKUP"
            )

        # Make repeat execution safe if the official table is already V2.
        if current_version == "V2":
            print("FACT_PRODUCTION_V2_ALREADY_PROMOTED", flush=True)
            print("FACT_PRODUCTION_V1_BACKUP_PRESERVED", flush=True)
            print("FACT_PRODUCTION_V2_PROMOTION_PASSED", flush=True)
            return

        # Iceberg Replace Table As Select (RTAS) atomically replaces the table
        # contents/schema. The separately verified V1 backup remains available.
        spark.sql(
            f"REPLACE TABLE {OLD_FACT} USING iceberg AS SELECT * FROM {STAGING}"
        )

        promoted = spark.table(OLD_FACT)
        if promoted.columns != V2_COLUMNS:
            raise RuntimeError("Promoted fact_production schema is not V2")
        assert_same_rows(
            promoted.select(*V2_DATA_COLUMNS),
            stage.select(*V2_DATA_COLUMNS),
            "PROMOTED_FACT_PRODUCTION_VS_V2_STAGING",
        )

        promoted_rows = promoted.count()
        promoted_unique = promoted.select("production_fact_key").distinct().count()
        if promoted_rows != EXPECTED_V2_ROWS or promoted_unique != promoted_rows:
            raise RuntimeError(
                f"Promoted Fact verification failed: rows={promoted_rows}; "
                f"unique={promoted_unique}"
            )

        backup_after = spark.table(V1_BACKUP)
        validate_v1_against_v2_legacy(
            spark, backup_after, promoted, "FINAL_V1_BACKUP"
        )

        print("FACT_PRODUCTION_V2_PROMOTED_ROWS:", promoted_rows, flush=True)
        print("FACT_PRODUCTION_V1_BACKUP_PRESERVED:", backup_after.count(), flush=True)
        print("FACT_PRODUCTION_V2_PROMOTION_PASSED", flush=True)
        print("V2_STAGING_RETAINED_FOR_ROLLBACK", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
