from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Futures-AsOf-Coverage")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/iceberg/main/")
    .getOrCreate()
)

try:
    coffee = (
        spark.table("lakehouse.silver.coffee_prices_daily")
        .select(
            F.col("quote_date").alias("market_date"),
            "province"
        )
        .distinct()
    )

    futures = (
        spark.table("lakehouse.silver.london_futures_daily")
        .select(
            F.col("trade_date").alias("futures_date"),
            "last_price",
            "quality_status"
        )
    )

    candidates = coffee.join(
        F.broadcast(futures),
        F.col("futures_date") <= F.col("market_date"),
        "left"
    )

    window = Window.partitionBy(
        "market_date", "province"
    ).orderBy(F.col("futures_date").desc_nulls_last())

    selected = (
        candidates
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .withColumn(
            "age_days",
            F.datediff("market_date", "futures_date")
        )
        .withColumn(
            "futures_status",
            F.when(F.col("futures_date").isNull(), "MISSING")
             .when(F.col("age_days") > 3, "STALE")
             .otherwise("VALID")
        )
    )

    print("COFFEE_KEYS:", coffee.count(), flush=True)

    print("FUTURES_COVERAGE:", flush=True)
    selected.groupBy("futures_status").count().orderBy(
        "futures_status"
    ).show(truncate=False)

    print("FUTURES_VALID_QUALITY:", flush=True)
    selected.filter(
        F.col("futures_status") == "VALID"
    ).groupBy("quality_status").count().show(truncate=False)

    print("FUTURES_ASOF_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
