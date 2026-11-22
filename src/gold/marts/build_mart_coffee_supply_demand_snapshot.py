#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Build DM12: Coffee Supply & Demand Snapshot.

Source:
    lakehouse.gold.fact_psd

Target:
    lakehouse.gold.mart_coffee_supply_demand_snapshot

Grain:
    One row per country + commodity + market year.

Important source interpretation:
    The MONTHLY PSD rows represent reporting/publication periods for estimates,
    not physical production or consumption occurring in those calendar months.
    The source audit found exactly one reporting period for each
    country + market year. Therefore this mart is a snapshot/estimate mart,
    not a revision-history mart. No revision columns are calculated.

Default mode is DRY RUN. Pass --write to create the Iceberg table.
The script refuses to overwrite an existing target table.
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "lakehouse.gold.fact_psd"
TARGET_TABLE = "lakehouse.gold.mart_coffee_supply_demand_snapshot"
COMMODITY_CODE = "0711100"
UNIT_ID = "02"

# USDA PSD attribute IDs confirmed by the source audit.
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
        F.col(numerator_col).isNotNull()
        & F.col(denominator_col).isNotNull()
        & (F.col(denominator_col) != 0),
        F.round(
            F.col(numerator_col) * F.lit(100.0) / F.col(denominator_col),
            3,
        ),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build monthly PSD estimate snapshot Gold Data Mart."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Create the Iceberg target table. Without this flag, run validation only.",
    )
    args = parser.parse_args()

    # Match the working Nessie REST + Iceberg catalog configuration used by DM11.
    spark = (
        SparkSession.builder
        .appName("TLCN-Build-Mart-Coffee-Supply-Demand-Snapshot")
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
        print("=" * 78)
        print("DM12 - COFFEE SUPPLY & DEMAND SNAPSHOT")
        print("SOURCE:", SOURCE_TABLE)
        print("TARGET:", TARGET_TABLE)
        print("GRAIN: country + commodity + market year")
        print("REPORTING PERIOD: retained as estimate/publication metadata")
        print("MODE:", "WRITE" if args.write else "DRY RUN")
        print("=" * 78)

        source = spark.table(SOURCE_TABLE)
        required = [
            "geography_key",
            "market_year_key",
            "calendar_year_key",
            "source_key",
            "country_code",
            "country_name",
            "commodity_code",
            "commodity_description",
            "market_year",
            "market_year_start",
            "market_year_end",
            "calendar_year",
            "month",
            "period_type",
            "attribute_id",
            "attribute_description",
            "unit_id",
            "unit_description",
            "value",
            "quality_status",
        ]
        require_columns(source, required)

        monthly = (
            source
            .filter(F.upper(F.trim(F.col("period_type"))) == "MONTHLY")
            .filter(F.col("commodity_code") == COMMODITY_CODE)
            .filter(F.col("unit_id") == UNIT_ID)
            .filter(F.col("attribute_id").isin(list(ATTRIBUTE_COLUMNS.keys())))
            .select(
                "geography_key",
                "market_year_key",
                "calendar_year_key",
                "source_key",
                "country_code",
                "country_name",
                "commodity_code",
                "commodity_description",
                "market_year",
                "market_year_start",
                "market_year_end",
                "calendar_year",
                "month",
                "attribute_id",
                "attribute_description",
                "unit_id",
                "unit_description",
                "value",
                "quality_status",
            )
            .withColumn("reporting_year", F.col("calendar_year").cast("int"))
            .withColumn("reporting_month", F.col("month").cast("int"))
        )

        source_rows = monthly.count()
        if source_rows == 0:
            raise RuntimeError(
                "No monthly PSD rows found for commodity 0711100 and unit 02."
            )

        invalid_period_rows = monthly.filter(
            F.col("reporting_year").isNull()
            | F.col("reporting_month").isNull()
            | (F.col("reporting_month") < 1)
            | (F.col("reporting_month") > 12)
        ).count()
        if invalid_period_rows:
            raise RuntimeError(
                "Invalid reporting year/month rows detected: "
                + str(invalid_period_rows)
            )

        null_source_keys = monthly.filter(
            F.col("country_code").isNull()
            | F.col("commodity_code").isNull()
            | F.col("market_year").isNull()
            | F.col("market_year_start").isNull()
            | F.col("reporting_year").isNull()
            | F.col("reporting_month").isNull()
            | F.col("attribute_id").isNull()
        ).count()
        if null_source_keys:
            raise RuntimeError(
                "Null monthly PSD natural-grain key rows detected: "
                + str(null_source_keys)
            )

        # The source audit established one reporting period per country + market
        # year. Recheck that assumption in the build so future source changes
        # cannot silently turn this snapshot mart into an invalid grain.
        monthly = monthly.withColumn(
            "_reporting_period_key",
            F.concat_ws(
                "-",
                F.col("reporting_year").cast("string"),
                F.lpad(F.col("reporting_month").cast("string"), 2, "0"),
            ),
        )

        country_market_year = ["country_code", "commodity_code", "market_year"]

        multiple_reporting_period_keys = (
            monthly.groupBy(*country_market_year)
            .agg(F.countDistinct("_reporting_period_key").alias("period_count"))
            .filter(F.col("period_count") != 1)
            .count()
        )
        if multiple_reporting_period_keys:
            raise RuntimeError(
                "Snapshot assumption violated: found country/market-year keys "
                "with zero or multiple reporting periods: "
                + str(multiple_reporting_period_keys)
                + ". Reassess the grain and build a true vintage-history mart instead."
            )

        natural_key = country_market_year + ["attribute_id"]
        duplicate_source_keys = (
            monthly.groupBy(*natural_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_source_keys:
            raise RuntimeError(
                "Duplicate source natural-grain keys detected: "
                + str(duplicate_source_keys)
            )

        aggregations = [
            F.max("geography_key").alias("geography_key"),
            F.max("market_year_key").alias("market_year_key"),
            F.max("calendar_year_key").alias("calendar_year_key"),
            F.max("source_key").alias("source_key"),
            F.max("country_name").alias("country_name"),
            F.max("commodity_description").alias("commodity_description"),
            F.max("market_year_start").alias("market_year_start"),
            F.max("market_year_end").alias("market_year_end"),
            F.max("reporting_year").alias("reporting_year"),
            F.max("reporting_month").alias("reporting_month"),
            F.max("_reporting_period_key").alias("reporting_period"),
            F.max("unit_id").alias("unit_id"),
            F.max("unit_description").alias("unit_description"),
            F.count(F.lit(1)).alias("source_record_count"),
            F.countDistinct("attribute_id").alias("attribute_count"),
            F.sum(
                F.when(
                    F.upper(
                        F.trim(F.coalesce(F.col("quality_status"), F.lit("UNKNOWN")))
                    ) != "PASS",
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

        mart = monthly.groupBy(*country_market_year).agg(*aggregations)

        mart = (
            mart
            .withColumn(
                "reporting_period_date",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("reporting_year").cast("string"),
                        F.lpad(F.col("reporting_month").cast("string"), 2, "0"),
                        F.lit("01"),
                    )
                ),
            )
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
                "import_share_of_total_supply_pct",
                safe_pct(
                    "imports_thousand_bags_60kg",
                    "total_supply_thousand_bags_60kg",
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
                "net_exports_thousand_bags_60kg",
                F.when(
                    F.col("exports_thousand_bags_60kg").isNotNull()
                    & F.col("imports_thousand_bags_60kg").isNotNull(),
                    F.round(
                        F.col("exports_thousand_bags_60kg")
                        - F.col("imports_thousand_bags_60kg"),
                        3,
                    ),
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
                F.when(
                    (F.col("quality_review_count") == 0)
                    & (F.col("attribute_count") == len(ATTRIBUTE_COLUMNS)),
                    F.lit("PASS"),
                ).otherwise(F.lit("REVIEW")),
            )
        )

        # Identify the latest available market year for each country. This is
        # descriptive metadata, not a claim that all countries share one
        # common reporting date.
        country_latest_window = Window.partitionBy("country_code", "commodity_code")
        mart = mart.withColumn(
            "is_latest_available_market_year",
            F.col("market_year_start")
            == F.max("market_year_start").over(country_latest_window),
        )

        mart = mart.withColumn(
            "supply_demand_snapshot_fact_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("country_code"),
                    F.col("commodity_code"),
                    F.col("market_year").cast("string"),
                ),
                256,
            ),
        )

        leading = [
            "supply_demand_snapshot_fact_key",
            "geography_key",
            "market_year_key",
            "calendar_year_key",
            "source_key",
            "country_code",
            "country_name",
            "commodity_code",
            "commodity_description",
            "market_year",
            "market_year_start",
            "market_year_end",
            "reporting_year",
            "reporting_month",
            "reporting_period",
            "reporting_period_date",
            "unit_id",
            "unit_description",
        ]
        derived = [
            "robusta_production_share_pct",
            "arabica_production_share_pct",
            "export_to_production_pct",
            "import_share_of_total_supply_pct",
            "stock_to_use_pct",
            "domestic_consumption_share_of_distribution_pct",
            "net_exports_thousand_bags_60kg",
            "supply_demand_balance_thousand_bags_60kg",
            "is_latest_available_market_year",
            "source_record_count",
            "attribute_count",
            "quality_review_count",
            "quality_status",
        ]
        mart = mart.select(
            *(leading + list(ATTRIBUTE_COLUMNS.values()) + derived)
        )

        # Target-grain and completeness validation.
        target_rows = mart.count()
        distinct_keys = mart.select(
            "country_code", "commodity_code", "market_year"
        ).distinct().count()
        duplicate_target_keys = target_rows - distinct_keys
        null_target_keys = mart.filter(
            F.col("country_code").isNull()
            | F.col("commodity_code").isNull()
            | F.col("market_year").isNull()
            | F.col("reporting_year").isNull()
            | F.col("reporting_month").isNull()
            | F.col("supply_demand_snapshot_fact_key").isNull()
        ).count()
        country_count = mart.select("country_code").distinct().count()
        reporting_period_count = mart.select("reporting_period").distinct().count()
        incomplete_attribute_rows = mart.filter(
            F.col("attribute_count") != len(ATTRIBUTE_COLUMNS)
        ).count()
        quality_review_rows = mart.filter(
            F.col("quality_status") != "PASS"
        ).count()
        latest_market_year_rows = mart.filter(
            F.col("is_latest_available_market_year")
        ).count()

        print("PSD_SNAPSHOT_SOURCE_ROWS:", source_rows)
        print("PSD_SNAPSHOT_TARGET_ROWS:", target_rows)
        print("PSD_SNAPSHOT_DISTINCT_GRAIN_KEYS:", distinct_keys)
        print("PSD_SNAPSHOT_DUPLICATE_GRAIN_KEYS:", duplicate_target_keys)
        print("PSD_SNAPSHOT_NULL_KEYS:", null_target_keys)
        print("PSD_SNAPSHOT_COUNTRY_COUNT:", country_count)
        print("PSD_SNAPSHOT_REPORTING_PERIOD_COUNT:", reporting_period_count)
        print("PSD_SNAPSHOT_INCOMPLETE_ATTRIBUTE_ROWS:", incomplete_attribute_rows)
        print("PSD_SNAPSHOT_QUALITY_REVIEW_ROWS:", quality_review_rows)
        print("PSD_SNAPSHOT_LATEST_MARKET_YEAR_ROWS:", latest_market_year_rows)

        if duplicate_target_keys != 0:
            raise RuntimeError("Target snapshot grain validation failed: duplicate keys.")
        if null_target_keys != 0:
            raise RuntimeError("Target snapshot grain validation failed: null keys.")
        if target_rows != distinct_keys:
            raise RuntimeError(
                "Target row count does not match distinct country/market-year grain."
            )
        if incomplete_attribute_rows != 0:
            raise RuntimeError(
                "Some country/market-year rows do not contain all 19 PSD attributes."
            )

        print("\n========== DM12 SAMPLE: VIETNAM ROBUSTA ESTIMATES ==========")
        mart.filter(F.col("country_code") == "VM").select(
            "country_code",
            "country_name",
            "market_year",
            "reporting_period",
            "robusta_production_thousand_bags_60kg",
            "production_thousand_bags_60kg",
            "robusta_production_share_pct",
            "ending_stocks_thousand_bags_60kg",
            "stock_to_use_pct",
            "quality_status",
        ).orderBy(F.col("market_year_start").desc()).show(30, truncate=False)

        print("\nPSD_SUPPLY_DEMAND_SNAPSHOT_VALIDATION_PASSED")

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
            print("PSD_SNAPSHOT_ICEBERG_ROWS:", written_rows)
            if written_rows != target_rows:
                raise RuntimeError(
                    "Iceberg row-count mismatch: expected "
                    + str(target_rows)
                    + ", got "
                    + str(written_rows)
                )
            print("PSD_SNAPSHOT_ICEBERG_WRITE_PASSED")
        else:
            print("PSD_SUPPLY_DEMAND_SNAPSHOT_DRY_RUN_PASSED")

        print("SPARK_JOB_PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
