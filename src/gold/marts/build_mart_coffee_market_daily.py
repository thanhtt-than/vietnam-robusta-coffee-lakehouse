"""Build Gold Coffee Market Daily Data Mart."""

from pyspark.sql import SparkSession

SOURCES = {
    "coffee": "lakehouse.silver.coffee_prices_daily",
    "weather": "lakehouse.silver.weather_daily",
    "fuel": "lakehouse.silver.fuel_price_observations",
    "futures": "lakehouse.silver.london_futures_daily",
    "news": "lakehouse.silver.vicofa_articles",
}

TARGET = "lakehouse.gold.mart_coffee_market_daily"


def create_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Build-Gold-Coffee-Market-Daily")
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


from pyspark.sql import Window
from pyspark.sql import functions as F

def prepare_coffee(spark):
    df = spark.table("lakehouse.silver.coffee_prices_daily")

    summary = df.groupBy("quote_date", "province").agg(
        F.countDistinct("source_family").alias("coffee_source_count"),
        F.countDistinct("price_vnd_kg").alias("coffee_distinct_price_count"),
        F.min("price_vnd_kg").alias("coffee_min_source_price"),
        F.max("price_vnd_kg").alias("coffee_max_source_price"),
    )

    priority = (
        F.when(F.col("source_family") == "bbw_coffee", 1)
        .when(F.col("source_family") == "legacy_api_json", 2)
        .when(F.col("source_family") == "enriched_csv", 3)
        .otherwise(99)
    )

    window = Window.partitionBy(
        "quote_date", "province"
    ).orderBy(
        F.col("source_priority"),
        F.col("selected_bronze_record_id").asc_nulls_last(),
    )

    selected = (
        df.withColumn("source_priority", priority)
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .join(summary, ["quote_date", "province"], "inner")
        .select(
            F.col("quote_date").alias("market_date"),
            "province",
            F.col("price_vnd_kg").alias("coffee_price_vnd_kg"),
            F.col("source_family").alias("coffee_source"),
            F.col("quality_status").alias("coffee_quality_status"),
            "coffee_source_count",
            "coffee_distinct_price_count",
            (F.col("coffee_distinct_price_count") > 1).alias(
                "price_conflict_flag"
            ),
            (
                F.col("coffee_max_source_price")
                - F.col("coffee_min_source_price")
            ).alias("price_spread_vnd_kg"),
        )
    )

    return selected


def prepare_weather(spark):
    df = spark.table("lakehouse.silver.weather_daily")

    priority = (
        F.when(
            (F.col("province") == "GIA_LAI") &
            (F.col("source") == "open_meteo_archive_gia_lai_corrected_v2"), 1
        )
        .when(
            (F.col("province") == "LAM_DONG") &
            (F.col("source") == "open_meteo_archive_lam_dong_corrected_v2"), 1
        )
        .when(
            (F.col("province") == "DAK_LAK") &
            (F.col("source") == "open_meteo_archive_dak_lak"), 1
        )
        .when(
            (F.col("province") == "DAK_NONG") &
            (F.col("source") == "open_meteo_archive_dak_nong"), 1
        )
        .when(
            F.col("province").isin("DAK_LAK", "DAK_NONG") &
            (F.col("source") == "open_meteo"), 2
        )
        .otherwise(99)
    )

    window = Window.partitionBy(
        "observation_date", "province"
    ).orderBy("weather_priority", "source_series_id")

    return (
        df.withColumn("weather_priority", priority)
        .filter(F.col("weather_priority") < 99)
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select(
            F.col("observation_date").alias("market_date"),
            "province",
            F.col("temperature_2m_mean").alias("temperature_mean_c"),
            F.col("rain_sum").alias("rain_mm"),
            F.col("precipitation_sum").alias("precipitation_mm"),
            F.col("source").alias("weather_source"),
            F.col("source_series_id").alias("weather_series_id"),
            F.col("quality_status").alias("weather_quality_status"),
        )
    )


def prepare_diesel(spark):
    df = spark.table("lakehouse.silver.fuel_price_observations")

    return (
        df.filter(
            F.col("series_code") == "FUEL/DO_0_05S_II_V1"
        )
        .groupBy("quote_date")
        .agg(
            F.min("price_vnd_per_unit").alias("diesel_price_vnd_l"),
            F.countDistinct("price_vnd_per_unit").alias(
                "diesel_distinct_prices"
            ),
        )
        .select(
            F.col("quote_date").alias("diesel_quote_date"),
            "diesel_price_vnd_l",
            "diesel_distinct_prices",
        )
    )

def join_diesel(mart, diesel):
    candidates = mart.join(
        F.broadcast(diesel),
        F.col("diesel_quote_date") <= F.col("market_date"),
        "left",
    )

    window = Window.partitionBy(
        "market_date", "province"
    ).orderBy(
        F.col("diesel_quote_date").desc_nulls_last()
    )

    return (
        candidates
        .withColumn("diesel_rn", F.row_number().over(window))
        .filter(F.col("diesel_rn") == 1)
        .drop("diesel_rn")
        .withColumn(
            "diesel_age_days",
            F.datediff("market_date", "diesel_quote_date"),
        )
        .withColumn(
            "diesel_status",
            F.when(F.col("diesel_quote_date").isNull(), "MISSING")
            .when(F.col("diesel_age_days") > 14, "STALE")
            .otherwise("VALID"),
        )
        .withColumn(
            "diesel_price_vnd_l",
            F.when(
                F.col("diesel_status") == "VALID",
                F.col("diesel_price_vnd_l"),
            ),
        )
    )


def prepare_futures(spark):
    return (
        spark.table("lakehouse.silver.london_futures_daily")
        .select(
            F.col("trade_date").alias("futures_trade_date"),
            F.col("last_price").alias("futures_last_price"),
            F.col("quality_status").alias("futures_quality_status"),
        )
    )


def join_futures(mart, futures):
    candidates = mart.join(
        F.broadcast(futures),
        F.col("futures_trade_date") <= F.col("market_date"),
        "left",
    )

    window = Window.partitionBy(
        "market_date", "province"
    ).orderBy(
        F.col("futures_trade_date").desc_nulls_last()
    )

    return (
        candidates
        .withColumn("futures_rn", F.row_number().over(window))
        .filter(F.col("futures_rn") == 1)
        .drop("futures_rn")
        .withColumn(
            "futures_age_days",
            F.datediff("market_date", "futures_trade_date"),
        )
        .withColumn(
            "futures_status",
            F.when(F.col("futures_trade_date").isNull(), "MISSING")
            .when(F.col("futures_age_days") > 3, "STALE")
            .otherwise("VALID"),
        )
        .withColumn(
            "futures_last_price",
            F.when(
                F.col("futures_status") == "VALID",
                F.col("futures_last_price"),
            ),
        )
    )


def prepare_news(spark):
    news = spark.table("lakehouse.silver.vicofa_articles")

    return (
        news.groupBy("published_date")
        .agg(
            F.count("*").alias("news_article_count"),
            F.sum(
                F.when(
                    F.col("primary_category") == "PRICE_MARKET", 1
                ).otherwise(0)
            ).alias("news_price_market_count"),
            F.sum(
                F.when(
                    F.col("primary_category") == "EXPORT_TRADE", 1
                ).otherwise(0)
            ).alias("news_export_trade_count"),
        )
        .withColumnRenamed("published_date", "market_date")
    )


def join_news(mart, news):
    return (
        mart.join(news, ["market_date"], "left")
        .fillna(
            0,
            subset=[
                "news_article_count",
                "news_price_market_count",
                "news_export_trade_count",
            ],
        )
        .withColumn(
            "news_has_articles",
            F.col("news_article_count") > 0,
        )
    )


def add_coffee_kpis(mart):
    window = Window.partitionBy("province").orderBy("market_date")
    ma7_window = window.rowsBetween(-6, 0)

    return (
        mart.withColumn(
            "coffee_previous_price_vnd_kg",
            F.lag("coffee_price_vnd_kg").over(window),
        )
        .withColumn(
            "coffee_change_vnd_kg",
            F.col("coffee_price_vnd_kg")
            - F.col("coffee_previous_price_vnd_kg"),
        )
        .withColumn(
            "coffee_change_pct",
            F.when(
                F.col("coffee_previous_price_vnd_kg") > 0,
                (
                    F.col("coffee_change_vnd_kg")
                    / F.col("coffee_previous_price_vnd_kg")
                ) * 100,
            ),
        )
        .withColumn(
            "coffee_ma7_vnd_kg",
            F.round(
                F.avg("coffee_price_vnd_kg").over(ma7_window),
                2,
            ),
        )
        .withColumn(
            "coffee_change_pct",
            F.round(F.col("coffee_change_pct"), 3).cast("decimal(12,3)"),
        )
        .drop("coffee_previous_price_vnd_kg")
    )


def main():
    spark = create_spark()

    try:
        mart = prepare_coffee(spark).join(
            prepare_weather(spark),
            ["market_date", "province"],
            "left",
        )

        mart = join_diesel(mart, prepare_diesel(spark))
        mart = join_futures(mart, prepare_futures(spark))
        mart = join_news(mart, prepare_news(spark))
        mart = add_coffee_kpis(mart)

        total = mart.count()
        duplicates = (
            mart.groupBy("market_date", "province")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("MART_ROWS:", total, flush=True)
        print("MART_DUPLICATE_KEYS:", duplicates, flush=True)

        if total != 5864 or duplicates != 0:
            raise RuntimeError("Gold Mart validation failed")

        if spark.catalog.tableExists(TARGET):
            raise RuntimeError(
                f"Target already exists; refusing to overwrite: {TARGET}"
            )

        print("GOLD_WRITE_START:", TARGET, flush=True)

        mart.writeTo(TARGET).using("iceberg").create()

        print("GOLD_TABLE_CREATED:", TARGET, flush=True)
        print("GOLD_WRITE_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
