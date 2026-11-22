#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export Gold Data Marts from Iceberg to local Parquet files for Streamlit.

This script is read-only with respect to Iceberg: it reads Gold tables and
overwrites only the dashboard/data/<mart_name> Parquet export directories.
"""

from pathlib import Path
from pyspark.sql import SparkSession

MARTS = [
    # Existing dashboard marts
    "mart_coffee_market_daily",
    "mart_coffee_price_trends",
    "mart_futures_domestic_spread",
    "mart_coffee_production_annual",
    "mart_coffee_export_annual",
    "mart_weather_price_impact",
    "mart_fuel_price_impact",
    "mart_market_news_analytics",
    # Final deep-analysis marts
    "mart_coffee_supply_demand",              # DM11
    "mart_robusta_fx_price_transmission",     # DM15
    "mart_robusta_ml_features_daily",         # DM16
]

OUTPUT = Path("/workspace/dashboard/data")


def main():
    spark = (
        SparkSession.builder
        .appName("Export-Gold-Marts-For-Streamlit")
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        OUTPUT.mkdir(parents=True, exist_ok=True)

        for name in MARTS:
            table = f"lakehouse.gold.{name}"
            destination = str(OUTPUT / name)

            print(f"EXPORT_START: {name}", flush=True)
            df = spark.table(table)
            expected = df.count()

            if expected == 0:
                raise RuntimeError(f"Empty table: {table}")

            df.write.mode("overwrite").parquet(destination)
            actual = spark.read.parquet(destination).count()

            if actual != expected:
                raise RuntimeError(
                    f"Export mismatch: {name}, expected={expected}, actual={actual}"
                )

            print(
                f"EXPORT_PASSED: {name} | ROWS: {actual}",
                flush=True,
            )

        print("ALL_11_DASHBOARD_EXPORTS_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
