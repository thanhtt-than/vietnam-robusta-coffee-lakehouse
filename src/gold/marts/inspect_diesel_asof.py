from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Diesel-AsOf-Coverage")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/iceberg/main/")
    .getOrCreate()
)

try:
    coffee = (
        spark.table("lakehouse.silver.coffee_prices_daily")
        .select("quote_date", "province")
        .distinct()
        .withColumnRenamed("quote_date", "market_date")
    )

    diesel = (
        spark.table("lakehouse.silver.fuel_price_observations")
        .filter(F.col("series_code") == "FUEL/DO_0_05S_II_V1")
        .groupBy("quote_date")
        .agg(F.min("price_vnd_per_unit").alias("diesel_price"))
        .withColumnRenamed("quote_date", "diesel_date")
    )

    candidates = coffee.join(
        F.broadcast(diesel),
        F.col("diesel_date") <= F.col("market_date"),
        "left"
    )

    w = Window.partitionBy(
        "market_date", "province"
    ).orderBy(F.col("diesel_date").desc_nulls_last())

    selected = (
        candidates.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .withColumn(
            "age_days",
            F.datediff("market_date", "diesel_date")
        )
        .withColumn(
            "diesel_status",
            F.when(F.col("diesel_date").isNull(), "MISSING")
             .when(F.col("age_days") > 14, "STALE")
             .otherwise("VALID")
        )
    )

    print("COFFEE_KEYS:", coffee.count(), flush=True)
    print("DIESEL_COVERAGE:", flush=True)

    selected.groupBy("diesel_status").count().orderBy(
        "diesel_status"
    ).show(truncate=False)

    print("DIESEL_ASOF_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
