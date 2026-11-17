from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Weather-Overlap")
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

    grouped = df.groupBy(
        "observation_date", "province"
    ).agg(
        F.count("*").alias("row_count"),
        F.countDistinct("source_series_id").alias("series_count")
    )

    print("UNIQUE_DATE_PROVINCE:", grouped.count(), flush=True)

    print(
        "MULTI_SERIES_KEYS:",
        grouped.filter(F.col("series_count") > 1).count(),
        flush=True
    )

    print("SERIES_DISTRIBUTION:", flush=True)
    grouped.groupBy("series_count").count().orderBy(
        "series_count"
    ).show(truncate=False)

    print("WEATHER_OVERLAP_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
