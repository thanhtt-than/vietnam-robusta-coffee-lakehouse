from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-A5-Verify-Gold")
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

# Table, primary key, expected rows
dimensions = [
    ("dim_date", "date_key", 9862),
    ("dim_year", "year_key", 27),
    ("dim_geography", "geography_key", 5),
    ("dim_source", "source_key", 14),
    ("dim_price_series", "price_series_key", 12),
    ("dim_weather_location", "weather_location_key", 12),
    ("dim_fuel_series", "fuel_series_key", 58),
    ("dim_destination", "destination_key", 162),
    ("dim_hs_product", "hs_product_key", 1),
    ("dim_article", "article_key", 1218),
    ("dim_news_topic", "topic_key", 9),
]

facts = [
    ("fact_domestic_price", "price_fact_key", 9409),
    ("fact_futures_price", "futures_fact_key", 4755),
    ("fact_weather_daily", "weather_fact_key", 97620),
    ("fact_fuel_price", "fuel_fact_key", 1704),
    ("fact_production", "production_fact_key", 19),
    ("fact_export", "export_fact_key", 3888),
    ("fact_news_topic", "news_topic_fact_key", 1472),
]

relationships = {
    "fact_domestic_price": [
        ("date_key", "dim_date", "date_key"),
        ("geography_key", "dim_geography", "geography_key"),
        ("source_key", "dim_source", "source_key"),
        ("price_series_key", "dim_price_series", "price_series_key"),
    ],
    "fact_futures_price": [
        ("trade_date_key", "dim_date", "date_key"),
        ("source_key", "dim_source", "source_key"),
    ],
    "fact_weather_daily": [
        ("date_key", "dim_date", "date_key"),
        ("geography_key", "dim_geography", "geography_key"),
        ("source_key", "dim_source", "source_key"),
        ("weather_location_key",
         "dim_weather_location", "weather_location_key"),
    ],
    "fact_fuel_price": [
        ("date_key", "dim_date", "date_key"),
        ("source_key", "dim_source", "source_key"),
        ("fuel_series_key", "dim_fuel_series", "fuel_series_key"),
    ],
    "fact_production": [
        ("year_key", "dim_year", "year_key"),
        ("geography_key", "dim_geography", "geography_key"),
        ("area_source_key", "dim_source", "source_key"),
        ("production_source_key", "dim_source", "source_key"),
    ],
    "fact_export": [
        ("year_key", "dim_year", "year_key"),
        ("geography_key", "dim_geography", "geography_key"),
        ("destination_key", "dim_destination", "destination_key"),
        ("hs_product_key", "dim_hs_product", "hs_product_key"),
        ("source_key", "dim_source", "source_key"),
    ],
    "fact_news_topic": [
        ("article_key", "dim_article", "article_key"),
        ("topic_key", "dim_news_topic", "topic_key"),
        ("date_key", "dim_date", "date_key"),
        ("source_key", "dim_source", "source_key"),
    ],
}

try:
    print("=== A5 TABLE AND PRIMARY KEY CHECK ===", flush=True)

    dimension_total = 0
    fact_total = 0

    for table, pk, expected in dimensions + facts:
        df = spark.table("lakehouse.gold." + table)

        count = df.count()
        unique = df.select(pk).distinct().count()
        nulls = df.filter(F.col(pk).isNull()).count()

        print(
            "TABLE:", table,
            "ROWS:", count,
            "UNIQUE_PK:", unique,
            "NULL_PK:", nulls,
            flush=True
        )

        assert count == expected
        assert unique == count
        assert nulls == 0

        if table.startswith("dim_"):
            dimension_total += count
        else:
            fact_total += count

    assert dimension_total == 11380
    assert fact_total == 118867

    print("GOLD_TABLES_AND_PK_PASSED", flush=True)

    print("=== A5 FOREIGN KEY CHECK ===", flush=True)

    for fact_name, links in relationships.items():
        fact = spark.table("lakehouse.gold." + fact_name)

        for fk, dim_name, dim_pk in links:
            dim = spark.table(
                "lakehouse.gold." + dim_name
            ).select(F.col(dim_pk).alias("_pk"))

            nulls = fact.filter(F.col(fk).isNull()).count()

            unmatched = (
                fact.filter(F.col(fk).isNotNull())
                .join(
                    dim,
                    F.col(fk) == F.col("_pk"),
                    "left_anti"
                )
                .count()
            )

            print(
                "FK:", fact_name + "." + fk,
                "NULL:", nulls,
                "UNMATCHED:", unmatched,
                flush=True
            )

            assert unmatched == 0

            # News date may be nullable only if source date is absent.
            if not (
                fact_name == "fact_news_topic"
                and fk == "date_key"
            ):
                assert nulls == 0

    print("GOLD_FOREIGN_KEYS_PASSED", flush=True)

    print("=== A5 BUSINESS RULE CHECK ===", flush=True)

    export = spark.table("lakehouse.gold.fact_export")

    assert export.filter(
        "is_world_aggregate = true"
    ).count() == 24

    assert export.filter(
        "is_world_aggregate = false"
    ).count() == 3864

    production = spark.table(
        "lakehouse.gold.fact_production"
    )

    assert production.filter("year_key = 2006").count() == 0

    news = spark.table("lakehouse.gold.fact_news_topic")

    assert news.select("article_key").distinct().count() == 1218
    assert news.count() == 1472

    weather = spark.table(
        "lakehouse.gold.fact_weather_daily"
    )

    assert weather.select(
        "weather_location_key"
    ).distinct().count() == 12

    print("GOLD_BUSINESS_RULES_PASSED", flush=True)

    print("GOLD_WAREHOUSE_A5_ALL_CHECKS_PASSED", flush=True)
    print("READ_ONLY_NO_TABLE_WRITTEN", flush=True)

finally:
    spark.stop()
