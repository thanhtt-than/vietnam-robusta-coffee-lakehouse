from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Gold-Dashboard-Latest")
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
    df = spark.table("lakehouse.gold.mart_coffee_market_daily")

    window = Window.partitionBy("province").orderBy(
        F.col("market_date").desc()
    )

    latest = (
        df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select(
            "province",
            "market_date",
            "coffee_price_vnd_kg",
            "coffee_change_vnd_kg",
            "coffee_change_pct",
            "coffee_ma7_vnd_kg",
            "coffee_source"
        )
        .orderBy("province")
    )

    print("LATEST_COFFEE_KPIS:", flush=True)
    latest.show(truncate=False)

    print("DASHBOARD_LATEST_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
