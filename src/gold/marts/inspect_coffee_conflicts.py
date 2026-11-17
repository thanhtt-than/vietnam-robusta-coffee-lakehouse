from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Coffee-Price-Conflicts")
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

    conflicts = (
        df.groupBy("quote_date", "province")
        .agg(
            F.countDistinct("price_vnd_kg").alias("distinct_prices"),
            F.min("price_vnd_kg").alias("min_price"),
            F.max("price_vnd_kg").alias("max_price")
        )
        .filter(F.col("distinct_prices") > 1)
        .withColumn(
            "difference_vnd_kg",
            F.col("max_price") - F.col("min_price")
        )
    )

    print("CONFLICT_COUNT:", conflicts.count(), flush=True)

    print("PRICE_DIFFERENCE_STATISTICS:", flush=True)
    conflicts.agg(
        F.min("difference_vnd_kg").alias("min_difference"),
        F.avg("difference_vnd_kg").alias("avg_difference"),
        F.max("difference_vnd_kg").alias("max_difference")
    ).show(truncate=False)

    print("LARGEST_CONFLICTS:", flush=True)
    conflicts.orderBy(
        F.desc("difference_vnd_kg")
    ).show(10, truncate=False)

    print("COFFEE_CONFLICT_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
