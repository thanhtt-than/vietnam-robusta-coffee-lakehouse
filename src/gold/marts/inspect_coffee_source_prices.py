from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Coffee-Source-Prices")
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
    df = spark.table("lakehouse.silver.coffee_prices_daily")

    dates = ["2024-12-03", "2025-07-10", "2025-11-17"]

    print("SOURCE_PRICE_COMPARISON:", flush=True)

    (
        df.filter(
            F.col("quote_date").cast("string").isin(dates)
        )
        .select(
            "quote_date",
            "province",
            "source_family",
            "price_vnd_kg",
            "quality_status"
        )
        .orderBy("quote_date", "province", "source_family")
        .show(100, truncate=False)
    )

    print("SOURCE_PRICE_COMPARISON_PASSED", flush=True)

finally:
    spark.stop()
