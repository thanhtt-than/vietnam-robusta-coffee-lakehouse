from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Recovered-Coffee-Trade")
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
    count = spark.table(
        "lakehouse.silver.coffee_trade_annual"
    ).count()

    print("RECOVERED_TRADE_ROWS:", count, flush=True)

    if count != 3888:
        raise RuntimeError(f"Expected 3888 rows, found {count}")

    print("RECOVERED_TRADE_VERIFIED", flush=True)

finally:
    spark.stop()
