from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Diesel-Variants")
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

    diesel = (
        df.filter(F.col("series_code").startswith("FUEL/DO_"))
        .groupBy("quote_date", "series_code")
        .agg(
            F.min("price_vnd_per_unit").alias("min_price"),
            F.max("price_vnd_per_unit").alias("max_price")
        )
    )

    diesel = diesel.withColumn(
        "base_series",
        F.regexp_replace("series_code", "_V[12]$", "")
    )

    comparison = (
        diesel.groupBy("quote_date", "base_series")
        .agg(
            F.count("*").alias("version_count"),
            F.min("min_price").alias("min_price"),
            F.max("max_price").alias("max_price")
        )
    )

    print(
        "V1_V2_PRICE_CONFLICTS:",
        comparison.filter(
            F.col("min_price") != F.col("max_price")
        ).count(),
        flush=True
    )

    print("DIESEL_RECENT_PRICES:", flush=True)

    (
        diesel.filter(F.col("quote_date") >= "2026-09-17")
        .orderBy("quote_date", "series_code")
        .show(100, truncate=False)
    )

    print("DIESEL_VARIANTS_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
