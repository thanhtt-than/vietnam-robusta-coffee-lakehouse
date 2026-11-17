from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Coffee-Overlap")
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

    grouped = df.groupBy("quote_date", "province").agg(
        F.count("*").alias("row_count"),
        F.countDistinct("source_family").alias("source_count"),
        F.countDistinct("price_vnd_kg").alias("distinct_prices")
    )

    print("UNIQUE_DATE_PROVINCE:", grouped.count(), flush=True)

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

    print("OVERLAP_DISTRIBUTION:", flush=True)
    grouped.groupBy("source_count").count().orderBy("source_count").show()

    print("COFFEE_OVERLAP_INSPECTION_PASSED", flush=True)
finally:
    spark.stop()
