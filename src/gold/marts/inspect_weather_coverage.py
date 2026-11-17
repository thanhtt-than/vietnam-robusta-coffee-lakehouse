from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Weather-Source-Coverage")
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
    df = spark.table("lakehouse.silver.weather_daily")

    print("WEATHER_SOURCE_COVERAGE:", flush=True)

    (
        df.groupBy("province", "source")
        .agg(
            F.countDistinct("observation_date").alias("days"),
            F.min("observation_date").alias("first_date"),
            F.max("observation_date").alias("last_date")
        )
        .orderBy("province", "source")
        .show(30, truncate=False)
    )

    print("WEATHER_COVERAGE_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
