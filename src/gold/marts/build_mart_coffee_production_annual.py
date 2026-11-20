"""Build/refresh the existing annual coffee production Data Mart from Fact V2.

Source:
  lakehouse.gold.fact_production (now the official V2 Fact)

Output:
  lakehouse.gold.mart_coffee_production_annual (existing official mart name)

Default: read-only dry run.
--write: back up the current mart (if present), then atomically replace the
existing mart with the V2-shaped result. No second official mart is created.
"""

import argparse

from pyspark.sql import SparkSession, functions as F


FACT = "lakehouse.gold.fact_production"
DIM_YEAR = "lakehouse.gold.dim_year"
DIM_GEOGRAPHY = "lakehouse.gold.dim_geography"
DIM_PRODUCT = "lakehouse.gold.dim_coffee_product"

TARGET = "lakehouse.gold.mart_coffee_production_annual"
BACKUP = "lakehouse.gold.mart_coffee_production_annual_v1_backup"

EXPECTED_FACT_ROWS = 141
EXPECTED_DATASETS = {
    "LEGACY_VIETNAM_ANNUAL": 19,
    "PROVINCE_STATISTICS": 65,
    "REGION_STATISTICS": 30,
    "VIETNAM_ANNUAL_STATISTICS": 11,
    "WORLD_MAJOR_COUNTRIES_2024": 16,
}

FACT_GRAIN = [
    "year_key",
    "geography_key",
    "coffee_product_key",
    "dataset_code",
    "area_measure_type",
    "publication_status_code",
]

FACT_REQUIRED_COLUMNS = [
    "production_fact_key",
    *FACT_GRAIN,
    "is_preliminary",
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
    "yield_method",
    "gold_processed_at",
]

# Keep all V2 lineage fields in the mart so the source can be traced.
FACT_LINEAGE_COLUMNS = [
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
]

MART_COLUMNS = [
    "production_fact_key",
    "year_key",
    "production_year",
    "geography_key",
    "geography_code",
    "geography_name",
    "geography_level",
    "region_name",
    "coffee_product_key",
    "product_code",
    "product_name",
    "variety",
    "grade",
    "is_variety_specified",
    "dataset_code",
    "area_measure_type",
    "is_preliminary",
    "publication_status_code",
    "coffee_area_thousand_ha",
    "coffee_production_thousand_tons",
    # Backward-compatible dashboard field: source yield if present,
    # otherwise calculated from production / area.
    "yield_tons_per_ha",
    "yield_source_reported_tons_per_ha",
    "yield_calculated_tons_per_ha",
    "yield_display_method",
    "area_growth_pct",
    "production_growth_pct",
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
    "yield_source_method",
    *FACT_LINEAGE_COLUMNS,
    "gold_processed_at",
]


def make_spark():
    spark = (
        SparkSession.builder
        .appName("Build-Mart-Coffee-Production-Annual-V2")
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


def require_table(spark, table_name):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required Iceberg table does not exist: {table_name}")


def assert_unique(df, keys, label):
    total = df.count()
    unique = df.select(*keys).distinct().count()
    if total != unique:
        print(f"{label}_DUPLICATE_SAMPLE:", flush=True)
        (
            df.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
            .show(20, truncate=False)
        )
        raise RuntimeError(
            f"{label}: duplicate key {keys}; duplicate_rows={total - unique}"
        )


def assert_same_rows(left, right, label):
    if left.columns != right.columns:
        raise RuntimeError(
            f"{label}: column/schema mismatch; "
            f"left={left.columns}; right={right.columns}"
        )
    left_only = left.exceptAll(right).limit(1).count()
    right_only = right.exceptAll(left).limit(1).count()
    if left_only or right_only:
        raise RuntimeError(
            f"{label}: row mismatch; left_only={left_only}; right_only={right_only}"
        )


def validate_dimension(df, key, label, required_columns):
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise RuntimeError(f"{label} missing columns: {missing}")
    assert_unique(df, [key], label)
    nulls = df.filter(F.col(key).isNull()).count()
    if nulls:
        raise RuntimeError(f"{label} has {nulls} NULL {key} values")


def build_mart(spark):
    for table in (FACT, DIM_YEAR, DIM_GEOGRAPHY, DIM_PRODUCT):
        require_table(spark, table)

    fact = spark.table(FACT)
    years = spark.table(DIM_YEAR)
    geography = spark.table(DIM_GEOGRAPHY)
    products = spark.table(DIM_PRODUCT)

    missing_fact = sorted(set(FACT_REQUIRED_COLUMNS) - set(fact.columns))
    missing_lineage = sorted(set(FACT_LINEAGE_COLUMNS) - set(fact.columns))
    if missing_fact or missing_lineage:
        raise RuntimeError(
            "fact_production is not the expected V2 schema. "
            f"missing_required={missing_fact}; missing_lineage={missing_lineage}"
        )

    validate_dimension(
        years,
        "year_key",
        "DIM_YEAR",
        ["year_key", "calendar_year"],
    )
    validate_dimension(
        geography,
        "geography_key",
        "DIM_GEOGRAPHY",
        [
            "geography_key",
            "geography_code",
            "geography_name",
            "geography_level",
            "region_name",
        ],
    )
    validate_dimension(
        products,
        "coffee_product_key",
        "DIM_COFFEE_PRODUCT",
        [
            "coffee_product_key",
            "product_code",
            "product_name",
            "variety",
            "grade",
            "is_variety_specified",
        ],
    )

    fact_rows = fact.count()
    unique_pk = fact.select("production_fact_key").distinct().count()
    unique_grain = fact.select(*FACT_GRAIN).distinct().count()

    print("PRODUCTION_FACT_ROWS:", fact_rows, flush=True)
    print("PRODUCTION_FACT_UNIQUE_PK:", unique_pk, flush=True)
    print("PRODUCTION_FACT_UNIQUE_GRAIN:", unique_grain, flush=True)

    if (
        fact_rows != EXPECTED_FACT_ROWS
        or unique_pk != fact_rows
        or unique_grain != fact_rows
    ):
        raise RuntimeError(
            "Official Fact V2 validation failed: "
            f"rows={fact_rows}; unique_pk={unique_pk}; unique_grain={unique_grain}"
        )

    dataset_counts = {
        row["dataset_code"]: row["count"]
        for row in fact.groupBy("dataset_code").count().collect()
    }
    print("PRODUCTION_FACT_DATASET_COUNTS:", dataset_counts, flush=True)
    if dataset_counts != EXPECTED_DATASETS:
        raise RuntimeError(f"Unexpected Fact V2 dataset distribution: {dataset_counts}")

    # Join only validated unique dimensions. This prevents row multiplication.
    base = (
        fact.alias("f")
        .join(
            years.select("year_key", "calendar_year").alias("y"),
            F.col("f.year_key") == F.col("y.year_key"),
            "left",
        )
        .join(
            geography.select(
                "geography_key",
                "geography_code",
                "geography_name",
                "geography_level",
                "region_name",
            ).alias("g"),
            F.col("f.geography_key") == F.col("g.geography_key"),
            "left",
        )
        .join(
            products.select(
                "coffee_product_key",
                "product_code",
                "product_name",
                "variety",
                "grade",
                "is_variety_specified",
            ).alias("p"),
            F.col("f.coffee_product_key") == F.col("p.coffee_product_key"),
            "left",
        )
    )

    selected_fact_columns = [F.col(f"f.{c}").alias(c) for c in fact.columns]
    base = base.select(
        *selected_fact_columns,
        F.col("y.calendar_year").alias("production_year"),
        F.col("g.geography_code").alias("geography_code"),
        F.col("g.geography_name").alias("geography_name"),
        F.col("g.geography_level").alias("geography_level"),
        F.col("g.region_name").alias("region_name"),
        F.col("p.product_code").alias("product_code"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.variety").alias("variety"),
        F.col("p.grade").alias("grade"),
        F.col("p.is_variety_specified").alias("is_variety_specified"),
        F.col("y.year_key").alias("_matched_year_key"),
        F.col("g.geography_key").alias("_matched_geography_key"),
        F.col("p.coffee_product_key").alias("_matched_product_key"),
    )

    missing_dimensions = base.filter(
        F.col("_matched_year_key").isNull()
        | F.col("_matched_geography_key").isNull()
        | F.col("_matched_product_key").isNull()
        | F.col("production_year").isNull()
        | F.col("geography_code").isNull()
        | F.col("product_code").isNull()
    ).count()

    if missing_dimensions:
        raise RuntimeError(
            f"Production mart has {missing_dimensions} unmatched Dimension rows"
        )

    mart = (
        base
        .withColumn(
            "yield_source_reported_tons_per_ha",
            F.col("yield_tons_per_ha"),
        )
        .withColumn(
            "yield_calculated_tons_per_ha",
            F.when(
                (F.col("coffee_area_thousand_ha") > 0)
                & F.col("coffee_production_thousand_tons").isNotNull(),
                F.round(
                    F.col("coffee_production_thousand_tons")
                    / F.col("coffee_area_thousand_ha"),
                    3,
                ),
            ).cast("decimal(18,3)"),
        )
        .withColumn(
            "yield_tons_per_ha",
            F.coalesce(
                F.col("yield_source_reported_tons_per_ha"),
                F.col("yield_calculated_tons_per_ha"),
            ).cast("decimal(18,3)"),
        )
        .withColumn(
            "yield_display_method",
            F.when(
                F.col("yield_source_reported_tons_per_ha").isNotNull(),
                F.coalesce(F.col("yield_method"), F.lit("SOURCE_REPORTED")),
            )
            .when(
                F.col("yield_calculated_tons_per_ha").isNotNull(),
                F.lit("CALCULATED_FROM_AREA_AND_PRODUCTION"),
            )
            .otherwise(F.lit("NOT_AVAILABLE")),
        )
        .withColumn(
            "yield_source_method",
            F.col("yield_method"),
        )
        .withColumn(
            "area_growth_pct",
            F.when(
                F.col("area_growth_index_prev_year_100").isNotNull(),
                F.round(F.col("area_growth_index_prev_year_100") - 100, 3),
            ).cast("decimal(18,3)"),
        )
        .withColumn(
            "production_growth_pct",
            F.when(
                F.col("production_growth_index_prev_year_100").isNotNull(),
                F.round(F.col("production_growth_index_prev_year_100") - 100, 3),
            ).cast("decimal(18,3)"),
        )
        .drop(
            "area_growth_index_prev_year_100",
            "production_growth_index_prev_year_100",
            "yield_method",
            "_matched_year_key",
            "_matched_geography_key",
            "_matched_product_key",
        )
        .select(*MART_COLUMNS)
    )

    mart_rows = mart.count()
    mart_unique_pk = mart.select("production_fact_key").distinct().count()
    mart_unique_grain = mart.select(*FACT_GRAIN).distinct().count()
    duplicate_grain_groups = (
        mart.groupBy(*FACT_GRAIN)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    negative_rows = mart.filter(
        (F.col("coffee_area_thousand_ha") < 0)
        | (F.col("coffee_production_thousand_tons") < 0)
        | (F.col("yield_source_reported_tons_per_ha") < 0)
        | (F.col("yield_calculated_tons_per_ha") < 0)
    ).count()
    null_required_rows = mart.filter(
        F.col("production_fact_key").isNull()
        | F.col("production_year").isNull()
        | F.col("geography_code").isNull()
        | F.col("product_code").isNull()
        | F.col("dataset_code").isNull()
        | F.col("area_measure_type").isNull()
        | F.col("publication_status_code").isNull()
    ).count()

    print("PRODUCTION_MART_ROWS:", mart_rows, flush=True)
    print("PRODUCTION_MART_UNIQUE_PK:", mart_unique_pk, flush=True)
    print("PRODUCTION_MART_UNIQUE_GRAIN:", mart_unique_grain, flush=True)
    print("PRODUCTION_MART_DUPLICATE_GRAIN_GROUPS:", duplicate_grain_groups, flush=True)
    print("PRODUCTION_MART_MISSING_DIMENSIONS:", missing_dimensions, flush=True)
    print("PRODUCTION_MART_NEGATIVE_MEASURE_ROWS:", negative_rows, flush=True)
    print("PRODUCTION_MART_NULL_REQUIRED_ROWS:", null_required_rows, flush=True)

    if (
        mart_rows != fact_rows
        or mart_unique_pk != mart_rows
        or mart_unique_grain != mart_rows
        or duplicate_grain_groups != 0
        or missing_dimensions != 0
        or negative_rows != 0
        or null_required_rows != 0
    ):
        raise RuntimeError("Production V2 Data Mart validation failed")

    mart.groupBy(
        "dataset_code",
        "area_measure_type",
        "publication_status_code",
    ).count().orderBy(
        "dataset_code",
        "area_measure_type",
        "publication_status_code",
    ).show(50, truncate=False)

    mart.select(
        "production_year",
        "geography_code",
        "dataset_code",
        "area_measure_type",
        "publication_status_code",
        "coffee_area_thousand_ha",
        "coffee_production_thousand_tons",
        "yield_source_reported_tons_per_ha",
        "yield_calculated_tons_per_ha",
        "yield_tons_per_ha",
        "yield_display_method",
    ).orderBy(
        "dataset_code",
        "production_year",
        "geography_code",
    ).show(50, truncate=False)

    print("PRODUCTION_MART_V2_VALIDATION_PASSED", flush=True)
    return mart


def write_existing_mart(spark, mart):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    mart.createOrReplaceTempView("stage_mart_coffee_production_annual_v2")

    target_exists = spark.catalog.tableExists(TARGET)
    backup_exists = spark.catalog.tableExists(BACKUP)

    if target_exists:
        current = spark.table(TARGET)

        # Idempotent rerun: if the official mart already matches the V2 output,
        # do not replace it again.
        if current.columns == MART_COLUMNS:
            assert_same_rows(
                current.select(*MART_COLUMNS),
                mart.select(*MART_COLUMNS),
                "CURRENT_PRODUCTION_MART_ALREADY_V2",
            )
            print("PRODUCTION_MART_ALREADY_MATCHES_V2", flush=True)
            return

        # Preserve the existing mart before changing its schema/data.
        if not backup_exists:
            spark.sql(
                f"CREATE TABLE {BACKUP} USING iceberg AS SELECT * FROM {TARGET}"
            )
            backup = spark.table(BACKUP)
            assert_same_rows(
                current.select(*current.columns),
                backup.select(*current.columns),
                "PRODUCTION_MART_V1_BACKUP",
            )
            print("PRODUCTION_MART_V1_BACKUP_VERIFIED_ROWS:", backup.count(), flush=True)
        else:
            backup = spark.table(BACKUP)
            if backup.columns != current.columns:
                raise RuntimeError(
                    "Existing production mart backup schema differs from the "
                    "current mart. Refusing to replace the target."
                )
            assert_same_rows(
                current.select(*current.columns),
                backup.select(*current.columns),
                "EXISTING_PRODUCTION_MART_V1_BACKUP",
            )
            print("EXISTING_PRODUCTION_MART_V1_BACKUP_VERIFIED", flush=True)

        spark.sql(
            f"REPLACE TABLE {TARGET} USING iceberg "
            "AS SELECT * FROM stage_mart_coffee_production_annual_v2"
        )
        print("PRODUCTION_MART_V1_REPLACED_BY_V2", flush=True)
    else:
        spark.sql(
            f"CREATE TABLE {TARGET} USING iceberg "
            "AS SELECT * FROM stage_mart_coffee_production_annual_v2"
        )
        print("PRODUCTION_MART_CREATED_FROM_V2", flush=True)

    saved = spark.table(TARGET)
    if saved.columns != MART_COLUMNS:
        raise RuntimeError("Official production mart schema does not match V2")
    assert_same_rows(
        saved.select(*MART_COLUMNS),
        mart.select(*MART_COLUMNS),
        "OFFICIAL_PRODUCTION_MART_V2",
    )

    saved_rows = saved.count()
    if saved_rows != EXPECTED_FACT_ROWS:
        raise RuntimeError(
            f"Official production mart row count mismatch: {saved_rows}"
        )

    print("PRODUCTION_MART_ICEBERG_ROWS:", saved_rows, flush=True)
    if target_exists:
        print("PRODUCTION_MART_V1_BACKUP_PRESERVED", flush=True)
    print("PRODUCTION_MART_V2_WRITE_PASSED", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Back up the current production mart and replace it with V2.",
    )
    args = parser.parse_args()

    spark = make_spark()
    try:
        mart = build_mart(spark)

        if args.write:
            write_existing_mart(spark, mart)
        else:
            print("PRODUCTION_MART_V2_DRY_RUN_PASSED", flush=True)
            print("NO_GOLD_MART_TABLE_WRITTEN", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
