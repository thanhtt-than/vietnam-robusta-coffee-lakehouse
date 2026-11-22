#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build DM11: mart_coffee_supply_demand

Source:
    lakehouse.gold.fact_psd

Target:
    lakehouse.gold.mart_coffee_supply_demand

Grain:
    One row per country + commodity + market year.
    Annual PSD observations only; no mixing with monthly records.

Default mode is DRY RUN. Pass --write to create the Iceberg table.
The script intentionally refuses to overwrite an existing target table.
"""

import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


SOURCE_TABLE = "lakehouse.gold.fact_psd"
TARGET_TABLE = "lakehouse.gold.mart_coffee_supply_demand"
COMMODITY_CODE = "0711100"
UNIT_ID = "02"

# USDA PSD attribute IDs confirmed in the source data audit.
ATTRIBUTE_COLUMNS = {
    "020": "beginning_stocks_thousand_bags_60kg",
    "028": "production_thousand_bags_60kg",
    "029": "arabica_production_thousand_bags_60kg",
    "053": "robusta_production_thousand_bags_60kg",
    "056": "other_production_thousand_bags_60kg",
    "057": "imports_thousand_bags_60kg",
    "058": "bean_imports_thousand_bags_60kg",
    "075": "roast_ground_imports_thousand_bags_60kg",
    "082": "soluble_imports_thousand_bags_60kg",
    "086": "total_supply_thousand_bags_60kg",
    "088": "exports_thousand_bags_60kg",
    "090": "bean_exports_thousand_bags_60kg",
    "107": "roast_ground_exports_thousand_bags_60kg",
    "114": "soluble_exports_thousand_bags_60kg",
    "125": "domestic_consumption_thousand_bags_60kg",
    "141": "roast_ground_domestic_consumption_thousand_bags_60kg",
    "154": "soluble_domestic_consumption_thousand_bags_60kg",
    "176": "ending_stocks_thousand_bags_60kg",
    "178": "total_distribution_thousand_bags_60kg",
}


def require_columns(df, required):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise RuntimeError(
            "Source schema is missing required columns: " + ", ".join(missing)
        )


def safe_pct(numerator_col, denominator_col):
    return F.when(
        F.col(denominator_col).isNotNull() & (F.col(denominator_col) != 0),
        F.round(
            F.col(numerator_col) * F.lit(100.0) / F.col(denominator_col),
            3,
        ),
    )


def yoy_pct(current_col, previous_col, consecutive_col):
    return F.when(
        F.col(consecutive_col)
        & F.col(previous_col).isNotNull()
        & (F.col(previous_col) != 0)
        & F.col(current_col).isNotNull(),
        F.round(
            (F.col(current_col) - F.col(previous_col))
            * F.lit(100.0)
            / F.col(previous_col),
            3,
        ),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build annual coffee supply-demand Gold Data Mart."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Create the Iceberg target table. Without this flag, run validation only.",
    )
    args = parser.parse_args()

    # Use the same Nessie REST + Iceberg catalog configuration as the
    # previously successful Gold Data Mart jobs. Without this, Spark falls
    # back to spark_catalog and rejects three-part lakehouse.gold.* names.
    spark = (
        SparkSession.builder
        .appName("TLCN-Build-Mart-Coffee-Supply-Demand")
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
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("=" * 76)
        print("DM11 - COFFEE SUPPLY & DEMAND")
        print("SOURCE:", SOURCE_TABLE)
        print("TARGET:", TARGET_TABLE)
        print("MODE:", "WRITE" if args.write else "DRY RUN")
        print("=" * 76)

        source = spark.table(SOURCE_TABLE)

        required_columns = [
            "geography_key",
            "country_code",
            "country_name",
            "commodity_code",
            "commodity_description",
            "market_year",
            "market_year_start",
            "market_year_end",
            "calendar_year",
            "period_type",
            "attribute_id",
            "attribute_description",
            "unit_id",
            "unit_description",
            "value",
            "quality_status",
        ]
        require_columns(source, required_columns)

        annual = (
            source
            .filter(F.upper(F.trim(F.col("period_type"))) == "ANNUAL")
            .filter(F.col("commodity_code") == COMMODITY_CODE)
            .filter(F.col("unit_id") == UNIT_ID)
            .filter(F.col("attribute_id").isin(list(ATTRIBUTE_COLUMNS.keys())))
            .select(
                "geography_key",
                "country_code",
                "country_name",
                "commodity_code",
                "commodity_description",
                "market_year",
                "market_year_start",
                "market_year_end",
                "calendar_year",
                "attribute_id",
                "unit_id",
                "unit_description",
                "value",
                "quality_status",
            )
        )

        source_rows = annual.count()
        if source_rows == 0:
            raise RuntimeError(
                "No annual PSD rows found for commodity 0711100 and unit 02."
            )

        # Check the target natural grain before pivoting. This is deliberately
        # stricter than checking geography_key alone: country + market year +
        # attribute must identify exactly one source observation.
        natural_key = [
            "country_code",
            "commodity_code",
            "market_year",
            "attribute_id",
        ]
        duplicate_source_keys = (
            annual.groupBy(*natural_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_source_keys:
            raise RuntimeError(
                "Duplicate source natural-grain keys detected: "
                + str(duplicate_source_keys)
            )

        null_source_keys = annual.filter(
            F.col("country_code").isNull()
            | F.col("commodity_code").isNull()
            | F.col("market_year").isNull()
            | F.col("market_year_start").isNull()
            | F.col("attribute_id").isNull()
        ).count()
        if null_source_keys:
            raise RuntimeError(
                "Null source natural-grain key rows detected: "
                + str(null_source_keys)
            )

        # One wide record per country and market year.
        grain_columns = [
            "country_code",
            "commodity_code",
            "market_year",
            "market_year_start",
            "market_year_end",
        ]

        aggregations = [
            F.max("geography_key").alias("geography_key"),
            F.max("country_name").alias("country_name"),
            F.max("commodity_description").alias("commodity_description"),
            F.max("calendar_year").alias("calendar_year"),
            F.max("unit_id").alias("unit_id"),
            F.max("unit_description").alias("unit_description"),
            F.count("*").alias("source_record_count"),
            F.countDistinct("attribute_id").alias("attribute_count"),
            F.sum(
                F.when(
                    F.upper(F.trim(F.coalesce(F.col("quality_status"), F.lit("UNKNOWN"))))
                    != "PASS",
                    1,
                ).otherwise(0)
            ).alias("quality_review_count"),
        ]

        for attribute_id, output_column in ATTRIBUTE_COLUMNS.items():
            aggregations.append(
                F.max(
                    F.when(
                        F.col("attribute_id") == attribute_id,
                        F.col("value"),
                    )
                ).alias(output_column)
            )

        mart = annual.groupBy(*grain_columns).agg(*aggregations)

        # Derived supply-demand indicators. All source values remain in
        # thousand 60-kg bags; percentage fields are expressed as percent.
        mart = (
            mart
            .withColumn(
                "robusta_production_share_pct",
                safe_pct(
                    "robusta_production_thousand_bags_60kg",
                    "production_thousand_bags_60kg",
                ),
            )
            .withColumn(
                "arabica_production_share_pct",
                safe_pct(
                    "arabica_production_thousand_bags_60kg",
                    "production_thousand_bags_60kg",
                ),
            )
            .withColumn(
                "export_to_production_pct",
                safe_pct(
                    "exports_thousand_bags_60kg",
                    "production_thousand_bags_60kg",
                ),
            )
            .withColumn(
                "stock_to_use_pct",
                safe_pct(
                    "ending_stocks_thousand_bags_60kg",
                    "total_distribution_thousand_bags_60kg",
                ),
            )
            .withColumn(
                "domestic_consumption_share_of_distribution_pct",
                safe_pct(
                    "domestic_consumption_thousand_bags_60kg",
                    "total_distribution_thousand_bags_60kg",
                ),
            )
            .withColumn(
                "supply_demand_balance_thousand_bags_60kg",
                F.when(
                    F.col("total_supply_thousand_bags_60kg").isNotNull()
                    & F.col("total_distribution_thousand_bags_60kg").isNotNull(),
                    F.round(
                        F.col("total_supply_thousand_bags_60kg")
                        - F.col("total_distribution_thousand_bags_60kg"),
                        3,
                    ),
                ),
            )
            .withColumn(
                "quality_status",
                F.when(F.col("quality_review_count") == 0, F.lit("PASS"))
                .otherwise(F.lit("REVIEW")),
            )
        )

        # Year-over-year metrics are calculated only when the immediately
        # preceding market-year start is present. This avoids comparing
        # non-consecutive observations as if they were adjacent years.
        window = (
            Window.partitionBy("country_code", "commodity_code")
            .orderBy("market_year_start")
        )

        yoy_metrics = {
            "production_thousand_bags_60kg": "production_yoy_pct",
            "robusta_production_thousand_bags_60kg": "robusta_production_yoy_pct",
            "exports_thousand_bags_60kg": "exports_yoy_pct",
            "imports_thousand_bags_60kg": "imports_yoy_pct",
            "domestic_consumption_thousand_bags_60kg": "domestic_consumption_yoy_pct",
            "ending_stocks_thousand_bags_60kg": "ending_stocks_yoy_pct",
        }

        mart = mart.withColumn(
            "_previous_market_year_start",
            F.lag("market_year_start").over(window),
        )

        consecutive = (
            F.col("_previous_market_year_start")
            == (F.col("market_year_start") - F.lit(1))
        )
        mart = mart.withColumn("_has_consecutive_previous_year", consecutive)

        previous_columns = []
        for source_column, output_column in yoy_metrics.items():
            previous_column = "_previous_" + source_column
            mart = mart.withColumn(
                previous_column,
                F.lag(source_column).over(window),
            )
            mart = mart.withColumn(
                output_column,
                yoy_pct(
                    source_column,
                    previous_column,
                    "_has_consecutive_previous_year",
                ),
            )
            previous_columns.append(previous_column)

        mart = mart.drop(
            "_previous_market_year_start",
            "_has_consecutive_previous_year",
            *previous_columns,
        )

        # Deterministic business key for downstream joins and auditing.
        mart = mart.withColumn(
            "supply_demand_fact_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("country_code"),
                    F.col("commodity_code"),
                    F.col("market_year"),
                ),
                256,
            ),
        )

        # Put identifiers and descriptive fields first.
        preferred_columns = [
            "supply_demand_fact_key",
            "geography_key",
            "country_code",
            "country_name",
            "commodity_code",
            "commodity_description",
            "market_year",
            "market_year_start",
            "market_year_end",
            "calendar_year",
            "unit_id",
            "unit_description",
        ] + list(ATTRIBUTE_COLUMNS.values()) + [
            "robusta_production_share_pct",
            "arabica_production_share_pct",
            "export_to_production_pct",
            "stock_to_use_pct",
            "domestic_consumption_share_of_distribution_pct",
            "supply_demand_balance_thousand_bags_60kg",
            "production_yoy_pct",
            "robusta_production_yoy_pct",
            "exports_yoy_pct",
            "imports_yoy_pct",
            "domestic_consumption_yoy_pct",
            "ending_stocks_yoy_pct",
            "source_record_count",
            "attribute_count",
            "quality_review_count",
            "quality_status",
        ]
        mart = mart.select(*preferred_columns)

        # Target-grain validation.
        target_rows = mart.count()
        target_distinct_keys = mart.select(
            "country_code", "commodity_code", "market_year"
        ).distinct().count()
        duplicate_target_keys = target_rows - target_distinct_keys
        null_target_keys = mart.filter(
            F.col("country_code").isNull()
            | F.col("commodity_code").isNull()
            | F.col("market_year").isNull()
            | F.col("supply_demand_fact_key").isNull()
        ).count()
        country_count = mart.select("country_code").distinct().count()
        incomplete_attribute_rows = mart.filter(
            F.col("attribute_count") != len(ATTRIBUTE_COLUMNS)
        ).count()
        quality_review_rows = mart.filter(
            F.col("quality_status") != "PASS"
        ).count()

        print("PSD_SOURCE_ROWS:", source_rows)
        print("PSD_TARGET_ROWS:", target_rows)
        print("PSD_TARGET_DISTINCT_GRAIN_KEYS:", target_distinct_keys)
        print("PSD_TARGET_DUPLICATE_GRAIN_KEYS:", duplicate_target_keys)
        print("PSD_TARGET_NULL_KEYS:", null_target_keys)
        print("PSD_COUNTRY_COUNT:", country_count)
        print("PSD_INCOMPLETE_ATTRIBUTE_ROWS:", incomplete_attribute_rows)
        print("PSD_QUALITY_REVIEW_ROWS:", quality_review_rows)

        if duplicate_target_keys != 0:
            raise RuntimeError("Target grain validation failed: duplicate keys.")
        if null_target_keys != 0:
            raise RuntimeError("Target grain validation failed: null keys.")
        if target_rows != target_distinct_keys:
            raise RuntimeError("Target row count does not match distinct grain count.")

        print("\n========== DM11 SAMPLE ==========")
        mart.orderBy(
            F.col("market_year_start").desc(),
            F.col("country_name").asc(),
        ).select(
            "market_year",
            "country_code",
            "country_name",
            "production_thousand_bags_60kg",
            "robusta_production_thousand_bags_60kg",
            "exports_thousand_bags_60kg",
            "domestic_consumption_thousand_bags_60kg",
            "ending_stocks_thousand_bags_60kg",
            "robusta_production_share_pct",
            "stock_to_use_pct",
            "supply_demand_balance_thousand_bags_60kg",
            "quality_status",
        ).show(25, truncate=False)

        print("\nPSD_SUPPLY_DEMAND_VALIDATION_PASSED")

        if args.write:
            if spark.catalog.tableExists(TARGET_TABLE):
                raise RuntimeError(
                    "Target table already exists; refusing to overwrite: "
                    + TARGET_TABLE
                )

            (
                mart.writeTo(TARGET_TABLE)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .create()
            )

            written_rows = spark.table(TARGET_TABLE).count()
            print("PSD_ICEBERG_ROWS:", written_rows)

            if written_rows != target_rows:
                raise RuntimeError(
                    "Iceberg row-count mismatch: expected "
                    + str(target_rows)
                    + ", got "
                    + str(written_rows)
                )

            print("PSD_ICEBERG_WRITE_PASSED")
        else:
            print("PSD_SUPPLY_DEMAND_DRY_RUN_PASSED")

        print("SPARK_JOB_PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
