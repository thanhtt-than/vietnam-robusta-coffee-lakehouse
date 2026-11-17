from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Verify-Gold-Mart")
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
    table = "lakehouse.gold.mart_coffee_market_daily"
    df = spark.table(table)

    rows = df.count()

    duplicates = (
        df.groupBy("market_date", "province")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("GOLD_TABLE:", table, flush=True)
    print("GOLD_ROWS:", rows, flush=True)
    print("GOLD_DUPLICATE_KEYS:", duplicates, flush=True)

    if rows != 5864 or duplicates != 0:
        raise RuntimeError("Gold Mart verification failed")

    print("GOLD_MART_VERIFIED", flush=True)

finally:
    spark.stop()
