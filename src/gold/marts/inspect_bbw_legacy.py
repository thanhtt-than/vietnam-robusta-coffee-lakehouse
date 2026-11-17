from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-BBW-Legacy-Agreement")
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

    bbw = (
        df.filter(F.col("source_family") == "bbw_coffee")
        .select(
            "quote_date",
            "province",
            F.col("price_vnd_kg").alias("bbw_price")
        )
    )

    legacy = (
        df.filter(F.col("source_family") == "legacy_api_json")
        .select(
            "quote_date",
            "province",
            F.col("price_vnd_kg").alias("legacy_price")
        )
    )

    joined = bbw.join(
        legacy, ["quote_date", "province"], "inner"
    )

    print("OVERLAPPING_KEYS:", joined.count(), flush=True)

    print(
        "MATCHING_PRICES:",
        joined.filter(
            F.col("bbw_price") == F.col("legacy_price")
        ).count(),
        flush=True
    )

    print(
        "DIFFERENT_PRICES:",
        joined.filter(
            ~F.col("bbw_price").eqNullSafe(F.col("legacy_price"))
        ).count(),
        flush=True
    )

    print("BBW_LEGACY_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
