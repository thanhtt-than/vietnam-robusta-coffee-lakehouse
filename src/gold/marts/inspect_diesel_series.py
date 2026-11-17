from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Diesel-Series")
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
    df = spark.table("lakehouse.silver.fuel_price_observations")

    diesel = df.filter(
        F.col("series_code").startswith("FUEL/DO_")
    )

    print("DIESEL_SERIES_COVERAGE:", flush=True)

    (
        diesel.groupBy("series_code")
        .agg(
            F.countDistinct("quote_date").alias("days"),
            F.min("quote_date").alias("first_date"),
            F.max("quote_date").alias("last_date"),
            F.countDistinct("price_vnd_per_unit").alias("distinct_prices")
        )
        .orderBy("series_code")
        .show(30, truncate=False)
    )

    print("DIESEL_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
