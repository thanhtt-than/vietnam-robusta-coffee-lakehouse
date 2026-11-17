from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-News-Spikes")
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
    news = spark.table("lakehouse.silver.vicofa_articles")

    print("TOP_NEWS_DATES:", flush=True)

    (
        news.groupBy("published_date")
        .agg(
            F.count("*").alias("total_articles"),
            F.sum(
                F.when(
                    F.col("primary_category") == "PRICE_MARKET", 1
                ).otherwise(0)
            ).alias("price_market_articles"),
            F.sum(
                F.when(
                    F.col("quality_status") != "PASS", 1
                ).otherwise(0)
            ).alias("review_articles")
        )
        .orderBy(F.desc("total_articles"), "published_date")
        .show(15, truncate=False)
    )

    print("NEWS_SPIKES_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
