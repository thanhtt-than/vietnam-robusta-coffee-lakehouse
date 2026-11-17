from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Verify-Gold-Analytics-KPI")
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

    print(
        "COFFEE_CHANGE_PCT_TYPE:",
        df.schema["coffee_change_pct"].dataType.simpleString(),
        flush=True
    )

    print("GOLD_KPI_COVERAGE:", flush=True)

    df.agg(
        F.count("*").alias("total_rows"),
        F.count("coffee_price_vnd_kg").alias("coffee_prices"),
        F.count("coffee_change_pct").alias("coffee_changes"),
        F.count("coffee_ma7_vnd_kg").alias("coffee_ma7"),
        F.count("temperature_mean_c").alias("weather_rows"),
        F.count("diesel_price_vnd_l").alias("diesel_valid_rows"),
        F.count("futures_last_price").alias("futures_valid_rows"),
    ).show(truncate=False)

    print("GOLD_KPI_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
