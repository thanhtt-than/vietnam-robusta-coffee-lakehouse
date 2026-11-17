from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Dashboard-Market-Summary")
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config(
        "spark.sql.catalog.lakehouse.uri",
        "http://nessie:19120/iceberg/main/"
    )
    .getOrCreate()
)

try:
    df = spark.table("lakehouse.gold.mart_coffee_market_daily")

    latest_date = df.agg(
        F.max("market_date")
    ).first()[0]

    latest = df.filter(F.col("market_date") == latest_date)

    print("DASHBOARD_LATEST_DATE:", latest_date, flush=True)

    print("DASHBOARD_MARKET_SUMMARY:", flush=True)

    latest.agg(
        F.count("*").alias("province_count"),
        F.round(
            F.avg("coffee_price_vnd_kg"), 2
        ).alias("avg_price_vnd_kg"),
        F.min("coffee_price_vnd_kg").alias("min_price_vnd_kg"),
        F.max("coffee_price_vnd_kg").alias("max_price_vnd_kg"),
        (
            F.max("coffee_price_vnd_kg")
            - F.min("coffee_price_vnd_kg")
        ).alias("price_spread_vnd_kg"),
        F.round(
            F.avg("coffee_change_pct"), 3
        ).alias("avg_change_pct"),
    ).show(truncate=False)

    print("DASHBOARD_SUMMARY_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
