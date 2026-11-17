from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-VICOFA-Daily-Coverage")
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

    daily = (
        news.groupBy("published_date")
        .agg(
            F.count("*").alias("article_count"),
            F.countDistinct("article_id").alias("unique_articles"),
            F.sum(
                F.when(
                    F.col("primary_category") == "PRICE_MARKET", 1
                ).otherwise(0)
            ).alias("price_market_count")
        )
    )

    print("NEWS_DATES:", daily.count(), flush=True)

    print(
        "MULTI_ARTICLE_DATES:",
        daily.filter(F.col("article_count") > 1).count(),
        flush=True
    )

    print(
        "ARTICLE_ID_DUPLICATE_DATES:",
        daily.filter(
            F.col("article_count") != F.col("unique_articles")
        ).count(),
        flush=True
    )

    print("NEWS_DAILY_STATISTICS:", flush=True)
    daily.agg(
        F.min("article_count").alias("min_articles"),
        F.avg("article_count").alias("avg_articles"),
        F.max("article_count").alias("max_articles")
    ).show(truncate=False)

    print("NEWS_DAILY_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
