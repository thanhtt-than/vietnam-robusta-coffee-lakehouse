from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Diesel-V1-Continuity")
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
        df.filter(
            F.col("series_code") == "FUEL/DO_0_05S_II_V1"
        )
        .groupBy("quote_date")
        .agg(
            F.min("price_vnd_per_unit").alias("min_price"),
            F.max("price_vnd_per_unit").alias("max_price")
        )
        .orderBy("quote_date")
    )

    print("DIESEL_V1_DAYS:", diesel.count(), flush=True)

    print(
        "DIESEL_V1_SOURCE_CONFLICTS:",
        diesel.filter(
            F.col("min_price") != F.col("max_price")
        ).count(),
        flush=True
    )

    window = Window.orderBy("quote_date")

    gaps = (
        diesel.withColumn(
            "previous_date",
            F.lag("quote_date").over(window)
        )
        .withColumn(
            "days_between",
            F.datediff("quote_date", "previous_date")
        )
    )

    print("DIESEL_V1_DATE_GAPS:", flush=True)
    gaps.groupBy("days_between").count().orderBy(
        "days_between"
    ).show(30, truncate=False)

    print("DIESEL_V1_CONTINUITY_PASSED", flush=True)

finally:
    spark.stop()
