"""Gold content dimensions from Silver sources; dry-run unless --write.

Dimension row counts are derived from current source data, not historical
fixed baselines. Primary/natural keys and referential checks remain enforced.
"""
import sys
from pyspark.sql import SparkSession, functions as F

WRITE = "--write" in sys.argv[1:]

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Content-Dimensions")
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


def hash_key(column):
    return F.expr(
        "CAST(CONV(SUBSTR(SHA2("
        + column +
        ", 256), 1, 15), 16, 10) AS BIGINT)"
    )


def verify(name, df, pk, natural_key):
    total = df.count()
    unique_pk = df.select(pk).distinct().count()
    unique_natural = df.select(natural_key).distinct().count()

    null_keys = df.filter(
        F.col(pk).isNull()
        | F.col(natural_key).isNull()
    ).count()

    print("DIMENSION:", name, flush=True)
    print("ROWS:", total, flush=True)
    print("UNIQUE_KEYS:", unique_pk, flush=True)
    print("UNIQUE_NATURAL_KEYS:", unique_natural, flush=True)
    print("NULL_KEYS:", null_keys, flush=True)

    if total <= 0:
        raise RuntimeError(f"{name}: dimension is empty")
    if total != unique_pk or total != unique_natural:
        raise RuntimeError(
            f"{name}: duplicate primary/natural keys "
            f"(rows={total}, unique_pk={unique_pk}, "
            f"unique_natural={unique_natural})"
        )
    if null_keys:
        raise RuntimeError(f"{name}: found {null_keys} null dimension keys")

    return total


TOPICS = [
    ("PRICE_MARKET", "Gia va thi truong"),
    ("EXPORT_TRADE", "Xuat nhap khau"),
    ("PRODUCTION_SUPPLY", "San xuat va nguon cung"),
    ("WEATHER_CLIMATE", "Thoi tiet va khi hau"),
    ("POLICY_REGULATION", "Chinh sach va quy dinh"),
    ("SUSTAINABILITY", "Phat trien ben vung"),
    ("TECHNOLOGY_PROCESSING", "Cong nghe va che bien"),
    ("ENTERPRISE_INVESTMENT", "Doanh nghiep va dau tu"),
    ("OTHER", "Khac")
]

try:
    trade = spark.table(
        "lakehouse.silver.coffee_trade_annual"
    )

    articles = spark.table(
        "lakehouse.silver.vicofa_articles"
    )

    # Destination: one record per partner code.
    destination = (
        trade.select(
            "partner_code",
            "partner_name",
            "is_world_aggregate"
        )
        .distinct()
        .withColumn(
            "destination_key",
            hash_key("partner_code")
        )
        .select(
            "destination_key",
            "partner_code",
            "partner_name",
            "is_world_aggregate"
        )
    )

    # HS product: keep leading zeros in code.
    hs_product = (
        trade.select(
            "product_code",
            "product_name"
        )
        .distinct()
        .withColumn(
            "hs_product_key",
            hash_key("product_code")
        )
        .select(
            "hs_product_key",
            "product_code",
            "product_name"
        )
    )

    # Article: one row per article ID. The row count is data-driven;
    # VICOFA ingestion can legitimately add articles over time.
    article = (
        articles.select(
            "article_id",
            F.col("url").alias("canonical_url"),
            "title",
            "summary",
            "published_date",
            "content_missing"
        )
        .distinct()
        .withColumn(
            "article_key",
            hash_key("article_id")
        )
        .select(
            "article_key",
            "article_id",
            "canonical_url",
            "title",
            "summary",
            "published_date",
            "content_missing"
        )
    )

    # Controlled vocabulary for keyword classification.
    topic_rows = [
        (index + 1, code, label, "KEYWORD_RULE_BASED")
        for index, (code, label) in enumerate(TOPICS)
    ]

    news_topic = spark.createDataFrame(
        topic_rows,
        """
        topic_key INT,
        topic_code STRING,
        topic_name STRING,
        classification_method STRING
        """
    )

    observed_topics = (
        articles.select(
            F.explode_outer(
                "keyword_categories"
            ).alias("topic_code")
        )
        .unionByName(
            articles.select(
                F.col("primary_category").alias("topic_code")
            )
        )
        .filter(F.col("topic_code").isNotNull())
        .distinct()
    )

    unknown_topics = (
        observed_topics.join(
            news_topic.select("topic_code"),
            on="topic_code",
            how="left_anti"
        )
        .count()
    )

    if unknown_topics:
        raise RuntimeError(
            f"Unknown news categories found: {unknown_topics}"
        )

    dimensions = {
        "dim_destination": (
            destination,
            "destination_key",
            "partner_code",
            """
            destination_key BIGINT,
            partner_code STRING,
            partner_name STRING,
            is_world_aggregate BOOLEAN
            """
        ),
        "dim_hs_product": (
            hs_product,
            "hs_product_key",
            "product_code",
            """
            hs_product_key BIGINT,
            product_code STRING,
            product_name STRING
            """
        ),
        "dim_article": (
            article,
            "article_key",
            "article_id",
            """
            article_key BIGINT,
            article_id STRING,
            canonical_url STRING,
            title STRING,
            summary STRING,
            published_date DATE,
            content_missing BOOLEAN
            """
        ),
        "dim_news_topic": (
            news_topic,
            "topic_key",
            "topic_code",
            """
            topic_key INT,
            topic_code STRING,
            topic_name STRING,
            classification_method STRING
            """
        )
    }

    counts = {}

    for name, (df, pk, natural, ddl) in dimensions.items():
        counts[name] = verify(
            name, df, pk, natural
        )

    # Article and HS product counts are source-driven, not fixed baselines.
    # The topic dimension is a controlled vocabulary defined by TOPICS.
    print(
        "SOURCE_ARTICLE_COUNT:",
        counts["dim_article"],
        flush=True
    )
    print(
        "SOURCE_HS_PRODUCT_COUNT:",
        counts["dim_hs_product"],
        flush=True
    )
    if counts["dim_news_topic"] != len(TOPICS):
        raise RuntimeError(
            "News topic dimension does not match the controlled TOPICS vocabulary"
        )

    world = destination.filter(
        F.col("partner_code") == "000"
    )

    if world.count() != 1:
        raise RuntimeError("Expected exactly one world aggregate destination")
    if world.first()["is_world_aggregate"] is not True:
        raise RuntimeError("World aggregate partner code 000 is not flagged correctly")

    print(
        "WORLD_AGGREGATE_VERIFIED",
        flush=True
    )

    print("DESTINATION_EXAMPLES:", flush=True)
    destination.orderBy("partner_code").show(
        10, truncate=False
    )

    print("NEWS_TOPICS:", flush=True)
    news_topic.orderBy("topic_key").show(
        20, truncate=False
    )

    print(
        "GOLD_CONTENT_DIMENSIONS_VALIDATED",
        flush=True
    )

    if WRITE:
        prerequisites = [
            "dim_date",
            "dim_year",
            "dim_geography",
            "dim_source",
            "dim_price_series",
            "dim_weather_location",
            "dim_fuel_series"
        ]

        for name in prerequisites:
            target = "lakehouse.gold." + name

            if not spark.catalog.tableExists(target):
                raise RuntimeError(
                    "Missing Gold prerequisite: " + target
                )

        print(
            "GOLD_PREREQUISITES_FOUND",
            flush=True
        )

        # Prevent article dates outside the Date dimension.
        known_dates = spark.table(
            "lakehouse.gold.dim_date"
        ).select("full_date")

        unmatched_dates = (
            article.select(
                F.col("published_date").alias("full_date")
            )
            .filter(F.col("full_date").isNotNull())
            .distinct()
            .join(
                known_dates,
                on="full_date",
                how="left_anti"
            )
            .count()
        )

        if unmatched_dates:
            raise RuntimeError(
                f"{unmatched_dates} article publication dates are missing "
                "from lakehouse.gold.dim_date"
            )

        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        for name, (df, pk, natural, ddl) in dimensions.items():
            target = "lakehouse.gold." + name
            stage = "stage_" + name

            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {target} "
                f"({ddl}) USING iceberg"
            )

            if spark.table(target).columns != df.columns:
                raise RuntimeError(
                    "Schema mismatch: " + target
                )

            df.createOrReplaceTempView(stage)

            spark.sql(f"""
                MERGE INTO {target} t
                USING {stage} s
                ON t.{natural} = s.{natural}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

            stored = spark.table(target)
            actual = stored.count()
            unique_pk = stored.select(pk).distinct().count()
            unique_natural = stored.select(natural).distinct().count()
            null_keys = stored.filter(
                F.col(pk).isNull() | F.col(natural).isNull()
            ).count()

            # Dimensions are maintained as durable lookup tables. A natural key
            # that disappears from today's Silver snapshot may still be referenced
            # by historical Gold facts, so do not delete it just to force row-count
            # equality. Instead, require every current source key to exist in Gold.
            source_keys = df.select(natural).distinct()
            stored_keys = stored.select(natural).distinct()

            missing_source_keys = (
                source_keys.join(
                    stored_keys,
                    on=natural,
                    how="left_anti"
                ).count()
            )

            retained_historical_keys = (
                stored_keys.join(
                    source_keys,
                    on=natural,
                    how="left_anti"
                )
            )

            retained_historical_count = retained_historical_keys.count()

            print(name.upper() + "_ICEBERG_TOTAL:", actual, flush=True)
            print(name.upper() + "_ICEBERG_SOURCE_TOTAL:", counts[name], flush=True)
            print(name.upper() + "_ICEBERG_UNIQUE_PK:", unique_pk, flush=True)
            print(name.upper() + "_ICEBERG_UNIQUE_NATURAL:", unique_natural, flush=True)
            print(name.upper() + "_ICEBERG_NULL_KEYS:", null_keys, flush=True)
            print(
                name.upper() + "_ICEBERG_MISSING_SOURCE_KEYS:",
                missing_source_keys,
                flush=True
            )
            print(
                name.upper() + "_ICEBERG_RETAINED_HISTORICAL_KEYS:",
                retained_historical_count,
                flush=True
            )

            if retained_historical_count:
                print(
                    name.upper() + "_HISTORICAL_KEYS_RETAINED_SAMPLE:",
                    flush=True
                )
                retained_historical_keys.orderBy(natural).show(
                    20, truncate=False
                )

            if missing_source_keys:
                raise RuntimeError(
                    f"{target}: {missing_source_keys} current source keys "
                    "are missing from Gold after MERGE"
                )
            if actual != unique_pk or actual != unique_natural or null_keys:
                raise RuntimeError(
                    f"{target}: Iceberg key integrity check failed"
                )
            if actual < counts[name]:
                raise RuntimeError(
                    f"{target}: Gold has fewer rows ({actual}) than the "
                    f"current source ({counts[name]})"
                )

            print(
                name.upper() + "_ICEBERG_PASSED",
                flush=True
            )

        print(
            "GOLD_CONTENT_DIMENSIONS_ICEBERG_PASSED",
            flush=True
        )
    else:
        print(
            "GOLD_CONTENT_DIMENSIONS_DRY_RUN_PASSED",
            flush=True
        )
        print(
            "NO_GOLD_TABLE_WRITTEN",
            flush=True
        )

finally:
    spark.stop()
