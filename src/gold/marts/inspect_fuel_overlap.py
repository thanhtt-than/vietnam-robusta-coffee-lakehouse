from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Fuel-Overlap")
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

    grouped = df.groupBy("quote_date", "series_code").agg(
        F.count("*").alias("row_count"),
        F.countDistinct("source_family").alias("source_count"),
        F.countDistinct("price_vnd_per_unit").alias("distinct_prices")
    )

    print("UNIQUE_DATE_SERIES:", grouped.count(), flush=True)

    print(
        "MULTI_SOURCE_KEYS:",
        grouped.filter(F.col("source_count") > 1).count(),
        flush=True
    )

    print(
        "PRICE_DISAGREEMENT_KEYS:",
        grouped.filter(F.col("distinct_prices") > 1).count(),
        flush=True
    )

    print("SOURCE_DISTRIBUTION:", flush=True)
    grouped.groupBy("source_count").count().orderBy(
        "source_count"
    ).show(truncate=False)

    print("FUEL_OVERLAP_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
