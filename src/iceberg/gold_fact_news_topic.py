import sys
from pyspark.sql import SparkSession, functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.fact_news_topic"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-News-Topic")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
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
    a = spark.table("lakehouse.silver.vicofa_articles")
    da = spark.table("lakehouse.gold.dim_article")
    dt = spark.table("lakehouse.gold.dim_news_topic")
    dd = spark.table("lakehouse.gold.dim_date")
    ds = spark.table("lakehouse.gold.dim_source")

    article_total = a.count()
    print("NEWS_SILVER_ARTICLES:", article_total, flush=True)
    assert article_total == 1218

    for name, df, key in [
        ("silver_articles", a, "article_id"),
        ("dim_article", da, "article_id"),
        ("dim_news_topic", dt, "topic_code"),
        ("dim_date", dd, "full_date"),
        ("dim_source", ds, "source_code")
    ]:
        assert df.count() == df.select(key).distinct().count(), name

    # Empty arrays are replaced by empty arrays, not NULL.
    empty = F.expr("CAST(array() AS array<string>)")

    topics = (
        a.withColumn(
            "_keywords",
            F.coalesce(F.col("keyword_categories"), empty)
        )
        .withColumn(
            "_primary",
            F.when(
                F.col("primary_category").isNotNull()
                & (F.trim("primary_category") != ""),
                F.array(F.col("primary_category"))
            ).otherwise(empty)
        )
        .withColumn(
            "_topics",
            F.array_distinct(
                F.concat(F.col("_keywords"), F.col("_primary"))
            )
        )
        .withColumn(
            "_topics",
            F.when(
                F.size("_topics") == 0,
                F.array(F.lit("OTHER"))
            ).otherwise(F.col("_topics"))
        )
        .withColumn(
            "topic_code",
            F.explode("_topics")
        )
        .filter(
            F.col("topic_code").isNotNull()
            & (F.trim("topic_code") != "")
        )
    )

    expanded_total = topics.count()
    unique_pairs = topics.select(
        "article_id", "topic_code"
    ).distinct().count()

    covered_articles = topics.select(
        "article_id"
    ).distinct().count()

    print("NEWS_EXPANDED_ROWS:", expanded_total, flush=True)
    print("NEWS_UNIQUE_ARTICLE_TOPIC:", unique_pairs, flush=True)
    print("NEWS_COVERED_ARTICLES:", covered_articles, flush=True)

    assert expanded_total == unique_pairs
    assert covered_articles == article_total

    # Check that every topic exists in the controlled vocabulary.
    unknown = topics.select("topic_code").distinct().join(
        dt.select("topic_code"),
        "topic_code", "left_anti"
    )

    unknown_count = unknown.count()
    print("NEWS_UNKNOWN_TOPICS:", unknown_count, flush=True)

    if unknown_count:
        unknown.show(20, truncate=False)

    assert unknown_count == 0

    # Join article, topic, date and source Dimensions.
    joined = (
        topics.alias("v")
        .join(
            da.select("article_id", "article_key").alias("a"),
            F.col("v.article_id") == F.col("a.article_id"),
            "left"
        )
        .join(
            dt.select("topic_code", "topic_key").alias("t"),
            F.col("v.topic_code") == F.col("t.topic_code"),
            "left"
        )
        .join(
            dd.select("full_date", "date_key").alias("d"),
            F.col("v.published_date") == F.col("d.full_date"),
            "left"
        )
        .join(
            ds.select("source_code", "source_key").alias("s"),
            F.col("s.source_code") == F.lit("vicofa_website"),
            "left"
        )
    )

    assert joined.count() == expanded_total

    for name, col in [
        ("article_key", "a.article_key"),
        ("topic_key", "t.topic_key"),
        ("source_key", "s.source_key")
    ]:
        missing = joined.filter(F.col(col).isNull()).count()
        print("UNMATCHED_FK:", name, missing, flush=True)
        assert missing == 0

    # Missing publication dates remain NULL; no fabricated dates.
    null_dates = joined.filter(
        F.col("v.published_date").isNull()
    ).count()

    unmatched_dates = joined.filter(
        F.col("v.published_date").isNotNull()
        & F.col("d.date_key").isNull()
    ).count()

    print("NEWS_NULL_PUBLISHED_DATES:", null_dates, flush=True)
    print("NEWS_UNMATCHED_DATES:", unmatched_dates, flush=True)

    assert unmatched_dates == 0

    business_id = F.concat_ws(
        "||",
        F.col("v.article_id"),
        F.col("v.topic_code")
    )

    fact = joined.select(
        F.conv(
            F.substring(F.sha2(business_id, 256), 1, 15),
            16, 10
        ).cast("bigint").alias("news_topic_fact_key"),

        F.col("a.article_key").cast("bigint").alias("article_key"),
        F.col("t.topic_key").cast("int").alias("topic_key"),
        F.col("d.date_key").cast("int").alias("date_key"),
        F.col("s.source_key").cast("bigint").alias("source_key"),

        (F.col("v.topic_code") == F.col("v.primary_category"))
        .alias("is_primary_topic"),

        F.lit(1).cast("int").alias("topic_article_count"),
        F.col("v.quality_status").alias("quality_status"),
        F.col("v.content_missing").alias("content_missing"),
        F.col("v.selected_bronze_record_id")
         .alias("selected_bronze_record_id"),
        F.col("v.selected_bronze_object_key")
         .alias("selected_bronze_object_key"),
        F.col("v.list_bronze_record_id")
         .alias("list_bronze_record_id"),

        F.current_timestamp().alias("gold_processed_at")
    ).cache()

    total = fact.count()
    unique = fact.select(
        "news_topic_fact_key"
    ).distinct().count()

    null_keys = fact.filter(
        F.col("news_topic_fact_key").isNull()
        | F.col("article_key").isNull()
        | F.col("topic_key").isNull()
        | F.col("source_key").isNull()
    ).count()

    null_lineage = fact.filter(
        F.col("selected_bronze_record_id").isNull()
    ).count()

    print("FACT_NEWS_TOPIC_ROWS:", total, flush=True)
    print("FACT_NEWS_TOPIC_UNIQUE_KEYS:", unique, flush=True)
    print("FACT_NEWS_TOPIC_NULL_KEYS:", null_keys, flush=True)
    print("FACT_NEWS_TOPIC_NULL_LINEAGE:", null_lineage, flush=True)

    assert total == unique == expanded_total
    assert null_keys == 0
    assert null_lineage == 0

    print("NEWS_TOPIC_DISTRIBUTION:", flush=True)

    fact.join(
        dt.select("topic_key", "topic_code"),
        "topic_key"
    ).groupBy("topic_code").count().orderBy(
        "topic_code"
    ).show(20, truncate=False)

    print("NEWS_QUALITY_STATUS:", flush=True)

    fact.groupBy("quality_status").count().show(
        truncate=False
    )

    print("FACT_NEWS_TOPIC_VALIDATED", flush=True)

    if WRITE:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                news_topic_fact_key BIGINT,
                article_key BIGINT,
                topic_key INT,
                date_key INT,
                source_key BIGINT,
                is_primary_topic BOOLEAN,
                topic_article_count INT,
                quality_status STRING,
                content_missing BOOLEAN,
                selected_bronze_record_id STRING,
                selected_bronze_object_key STRING,
                list_bronze_record_id STRING,
                gold_processed_at TIMESTAMP
            ) USING iceberg
        """)

        existing = spark.table(TARGET)

        assert existing.columns == fact.columns, (
            "News Fact column mismatch"
        )

        assert (
            [x.dataType for x in existing.schema]
            == [x.dataType for x in fact.schema]
        ), "News Fact type mismatch"

        fact.createOrReplaceTempView("stage_fact_news_topic")

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_news_topic s
            ON t.news_topic_fact_key = s.news_topic_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        saved = spark.table(TARGET)
        saved_count = saved.count()
        saved_unique = saved.select(
            "news_topic_fact_key"
        ).distinct().count()

        print(
            "FACT_NEWS_TOPIC_ICEBERG_TOTAL:",
            saved_count, flush=True
        )

        assert saved_count == saved_unique == total

        print("FACT_NEWS_TOPIC_ICEBERG_PASSED", flush=True)

    else:
        print("FACT_NEWS_TOPIC_DRY_RUN_PASSED", flush=True)
        print("NO_GOLD_FACT_WRITTEN", flush=True)

finally:
    spark.stop()
