import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "lakehouse.gold.mart_market_news_analytics"


def main():
    spark = (
        SparkSession.builder
        .appName("Build-Mart-Market-News-Analytics")
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
    spark.sparkContext.setLogLevel("ERROR")

    try:
        fact = spark.table("lakehouse.gold.fact_news_topic")
        articles = spark.table("lakehouse.gold.dim_article")
        topics = spark.table("lakehouse.gold.dim_news_topic")
        dates = spark.table("lakehouse.gold.dim_date")

        for name, df, key in [
            ("dim_article", articles, "article_key"),
            ("dim_news_topic", topics, "topic_key"),
            ("dim_date", dates, "date_key"),
        ]:
            if df.count() != df.select(key).distinct().count():
                raise RuntimeError("Duplicate Dimension key: " + name)

        # 1. Join Fact with Dimensions.
        joined = (
            fact.alias("f")
            .join(
                articles.alias("a"),
                F.col("f.article_key") == F.col("a.article_key"),
                "left"
            )
            .join(
                topics.alias("t"),
                F.col("f.topic_key") == F.col("t.topic_key"),
                "left"
            )
            .join(
                dates.alias("d"),
                F.col("f.date_key") == F.col("d.date_key"),
                "left"
            )
            .select(
                F.col("f.news_topic_fact_key"),
                F.col("f.article_key"),
                F.col("f.topic_key"),
                F.col("t.topic_code"),
                F.col("t.topic_name"),
                F.col("t.classification_method"),
                F.col("d.full_date").alias("market_date"),
                F.col("f.is_primary_topic"),
                F.col("f.content_missing"),
                F.col("f.quality_status")
            )
        )

        if joined.count() != fact.count():
            raise RuntimeError("Fact/Dimension JOIN changed row count")

        missing_dimensions = joined.filter(
            F.col("market_date").isNull()
            | F.col("topic_code").isNull()
            | F.col("article_key").isNull()
        ).count()

        if missing_dimensions:
            raise RuntimeError(
                f"Missing Dimension references: {missing_dimensions}"
            )

        # 2. Deduplicate article-topic-date relationships.
        relations = (
            joined.groupBy(
                "market_date",
                "topic_key",
                "topic_code",
                "topic_name",
                "classification_method",
                "article_key"
            )
            .agg(
                F.max(
                    F.coalesce(
                        F.col("is_primary_topic").cast("int"),
                        F.lit(0)
                    )
                ).alias("primary_flag"),
                F.max(
                    F.coalesce(
                        F.col("content_missing").cast("int"),
                        F.lit(0)
                    )
                ).alias("missing_flag"),
                F.max(
                    F.when(
                        F.col("quality_status") != "PASS", 1
                    ).otherwise(0)
                ).alias("review_flag")
            )
        )

        # 3. Count unique articles per calendar day.
        daily_totals = (
            relations.groupBy("market_date")
            .agg(
                F.countDistinct("article_key").alias(
                    "daily_unique_article_count"
                )
            )
        )

        daily_topics = (
            relations.groupBy(
                "market_date",
                "topic_key",
                "topic_code",
                "topic_name",
                "classification_method"
            )
            .agg(
                F.countDistinct("article_key").alias(
                    "article_count"
                ),
                F.sum("primary_flag").alias(
                    "primary_article_count"
                ),
                F.sum("missing_flag").alias(
                    "content_missing_count"
                ),
                F.sum("review_flag").alias(
                    "quality_review_count"
                )
            )
        )

        # 4. Calendar scaffold: include zero-news days
        # across the observed Fact date range, for every topic.
        bounds = fact.join(
            dates,
            "date_key",
            "inner"
        ).agg(
            F.min("full_date").alias("min_date"),
            F.max("full_date").alias("max_date")
        ).first()

        if bounds["min_date"] is None:
            raise RuntimeError("No news dates found")

        calendar = (
            spark.range(1)
            .select(
                F.explode(
                    F.sequence(
                        F.lit(bounds["min_date"]),
                        F.lit(bounds["max_date"])
                    )
                ).alias("market_date")
            )
        )

        topic_list = topics.select(
            "topic_key",
            "topic_code",
            "topic_name",
            "classification_method"
        )

        scaffold = calendar.crossJoin(topic_list)

        mart = (
            scaffold
            .join(
                daily_topics,
                [
                    "market_date",
                    "topic_key",
                    "topic_code",
                    "topic_name",
                    "classification_method"
                ],
                "left"
            )
            .join(
                daily_totals,
                "market_date",
                "left"
            )
            .fillna(
                0,
                subset=[
                    "article_count",
                    "primary_article_count",
                    "content_missing_count",
                    "quality_review_count",
                    "daily_unique_article_count"
                ]
            )
        )

        # 5. Daily KPI and trailing calendar-day average.
        w = (
            Window.partitionBy("topic_key")
            .orderBy("market_date")
        )

        w7 = w.rowsBetween(-6, 0)

        mart = (
            mart
            .withColumn(
                "previous_day_article_count",
                F.lag("article_count").over(w)
            )
            .withColumn(
                "article_change_pct",
                F.when(
                    F.col("previous_day_article_count") > 0,
                    F.round(
                        (
                            F.col("article_count")
                            / F.col("previous_day_article_count") - 1
                        ) * 100,
                        3
                    )
                )
            )
            .withColumn(
                "article_ma7",
                F.round(
                    F.avg("article_count").over(w7), 3
                )
            )
            .withColumn(
                "topic_share_pct",
                F.when(
                    F.col("daily_unique_article_count") > 0,
                    F.round(
                        F.col("article_count")
                        / F.col("daily_unique_article_count")
                        * 100,
                        3
                    )
                )
            )
            .drop("previous_day_article_count")
        )

        # 6. Quality checks.
        total_rows = mart.count()
        duplicate_keys = (
            mart.groupBy("market_date", "topic_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid_counts = mart.filter(
            (F.col("article_count") < 0)
            | (F.col("primary_article_count") > F.col("article_count"))
            | (
                F.col("content_missing_count")
                > F.col("article_count")
            )
            | (
                F.col("quality_review_count")
                > F.col("article_count")
            )
        ).count()

        invalid_shares = mart.filter(
            (F.col("topic_share_pct") < 0)
            | (F.col("topic_share_pct") > 100)
        ).count()

        if (
            total_rows == 0
            or duplicate_keys
            or invalid_counts
            or invalid_shares
        ):
            raise RuntimeError(
                f"News Mart validation failed: rows={total_rows}, "
                f"duplicates={duplicate_keys}, "
                f"invalid_counts={invalid_counts}, "
                f"invalid_shares={invalid_shares}"
            )

        print("NEWS_FACT_ROWS:", fact.count(), flush=True)
        print("NEWS_MART_ROWS:", total_rows, flush=True)
        print("NEWS_DUPLICATES:", duplicate_keys, flush=True)
        print("NEWS_INVALID_COUNTS:", invalid_counts, flush=True)
        print("NEWS_MART_VALIDATION_PASSED", flush=True)

        mart.select(
            "market_date",
            "topic_name",
            "article_count",
            "primary_article_count",
            "daily_unique_article_count",
            "article_ma7",
            "topic_share_pct"
        ).orderBy(
            F.col("market_date").desc(),
            F.col("article_count").desc()
        ).show(20, truncate=False)

        # 7. Write only when explicitly requested.
        if "--write" in sys.argv[1:]:
            if spark.catalog.tableExists(TARGET):
                raise RuntimeError("Target already exists: " + TARGET)

            mart.writeTo(TARGET).using("iceberg").create()

            actual = spark.table(TARGET).count()
            if actual != total_rows:
                raise RuntimeError("Iceberg row count mismatch")

            print("NEWS_ICEBERG_ROWS:", actual, flush=True)
            print("NEWS_ICEBERG_WRITE_PASSED", flush=True)
        else:
            print("NEWS_MART_DRY_RUN_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
