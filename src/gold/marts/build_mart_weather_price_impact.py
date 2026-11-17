import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "lakehouse.gold.mart_weather_price_impact"


def main():
    spark = (
        SparkSession.builder
        .appName("Build-Mart-Weather-Price-Impact")
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
        prices = spark.table("lakehouse.gold.fact_domestic_price")
        weather = spark.table("lakehouse.gold.fact_weather_daily")
        dates = spark.table("lakehouse.gold.dim_date")
        geography = spark.table("lakehouse.gold.dim_geography")
        series = spark.table("lakehouse.gold.dim_price_series")
        locations = spark.table("lakehouse.gold.dim_weather_location")

        for name, df, key in [
            ("dim_date", dates, "date_key"),
            ("dim_geography", geography, "geography_key"),
            ("dim_price_series", series, "price_series_key"),
            ("dim_weather_location", locations, "weather_location_key"),
        ]:
            if df.count() != df.select(key).distinct().count():
                raise RuntimeError("Duplicate Dimension key: " + name)

        # 1. Domestic coffee prices.
        domestic = (
            prices.alias("p")
            .join(
                dates.alias("d"),
                F.col("p.date_key") == F.col("d.date_key")
            )
            .join(
                geography.alias("g"),
                F.col("p.geography_key")
                == F.col("g.geography_key")
            )
            .join(
                series.alias("s"),
                F.col("p.price_series_key")
                == F.col("s.price_series_key")
            )
            .select(
                F.col("p.price_fact_key"),
                F.col("d.full_date").alias("market_date"),
                F.col("p.geography_key"),
                F.col("g.geography_code").alias("province"),
                F.col("p.price_series_key"),
                F.col("s.source_family"),
                F.col("p.price_vnd_kg").alias(
                    "coffee_price_vnd_kg"
                ),
                F.col("p.quality_status").alias(
                    "price_quality_status"
                )
            )
        )

        price_window = (
            Window.partitionBy("price_series_key")
            .orderBy("market_date")
        )

        domestic = (
            domestic
            .withColumn(
                "previous_price",
                F.lag("coffee_price_vnd_kg").over(price_window)
            )
            .withColumn(
                "price_change_pct",
                F.when(
                    F.col("previous_price") > 0,
                    F.round(
                        (
                            F.col("coffee_price_vnd_kg")
                            / F.col("previous_price") - 1
                        ) * 100,
                        3
                    )
                )
            )
            .drop("previous_price")
        )

        # 2. Join weather observations with location metadata.
        weather_base = (
            weather.alias("w")
            .join(
                dates.alias("d"),
                F.col("w.date_key") == F.col("d.date_key")
            )
            .join(
                locations.alias("l"),
                F.col("w.weather_location_key")
                == F.col("l.weather_location_key")
            )
            .select(
                F.col("d.full_date").alias("market_date"),
                F.col("w.geography_key"),
                F.col("w.weather_location_key"),
                F.coalesce(
                    F.col("l.is_corrected"),
                    F.lit(False)
                ).alias("is_corrected"),
                F.col("w.temperature_2m_mean"),
                F.col("w.rain_sum"),
                F.col("w.precipitation_sum"),
                F.col("w.quality_status").alias(
                    "weather_quality_status"
                )
            )
        )

        # 3. Prefer corrected weather series on each date/province.
        # Aggregate remaining stations to one weather row.
        priority_window = Window.partitionBy(
            "market_date", "geography_key"
        )

        weather_selected = (
            weather_base
            .withColumn(
                "priority",
                F.max(
                    F.when(F.col("is_corrected"), 1).otherwise(0)
                ).over(priority_window)
            )
            .filter(
                F.when(F.col("is_corrected"), 1).otherwise(0)
                == F.col("priority")
            )
        )

        weather_daily = (
            weather_selected
            .groupBy("market_date", "geography_key")
            .agg(
                F.round(
                    F.avg("temperature_2m_mean"), 3
                ).alias("temperature_mean_c"),
                F.round(
                    F.avg("rain_sum"), 3
                ).alias("rain_sum_mm"),
                F.round(
                    F.avg("precipitation_sum"), 3
                ).alias("precipitation_mm"),
                F.countDistinct(
                    "weather_location_key"
                ).alias("weather_station_count"),
                F.max(
                    F.col("is_corrected").cast("int")
                ).cast("boolean").alias("weather_is_corrected"),
                F.sum(
                    F.when(
                        F.col("weather_quality_status") != "PASS",
                        1
                    ).otherwise(0)
                ).alias("weather_review_count")
            )
        )

        # 4. Join price with province-level weather.
        base = domestic.join(
            weather_daily,
            ["market_date", "geography_key"],
            "left"
        )

        # 5. Rolling statistical correlations (30 observations).
        # Correlation does not establish causation.
        w30 = (
            Window.partitionBy("price_series_key")
            .orderBy("market_date")
            .rowsBetween(-29, 0)
        )

        mart = (
            base
            .withColumn(
                "rain_price_corr30",
                F.round(
                    F.corr(
                        "rain_sum_mm",
                        "price_change_pct"
                    ).over(w30),
                    4
                )
            )
            .withColumn(
                "temperature_price_corr30",
                F.round(
                    F.corr(
                        "temperature_mean_c",
                        "price_change_pct"
                    ).over(w30),
                    4
                )
            )
            .drop("geography_key")
        )

        # 6. Data quality checks.
        price_rows = prices.count()
        mart_rows = mart.count()

        duplicates = (
            mart.groupBy(
                "market_date", "province", "price_series_key"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid_prices = mart.filter(
            F.col("coffee_price_vnd_kg").isNull()
            | (F.col("coffee_price_vnd_kg") <= 0)
        ).count()

        invalid_correlations = mart.filter(
            (F.abs(F.col("rain_price_corr30")) > 1)
            | (F.abs(F.col("temperature_price_corr30")) > 1)
        ).count()

        weather_coverage = mart.filter(
            F.col("temperature_mean_c").isNotNull()
            | F.col("rain_sum_mm").isNotNull()
        ).count()

        if (
            mart_rows != price_rows
            or duplicates != 0
            or invalid_prices != 0
            or invalid_correlations != 0
        ):
            raise RuntimeError(
                f"Validation failed: source={price_rows}, "
                f"mart={mart_rows}, duplicates={duplicates}, "
                f"invalid_prices={invalid_prices}, "
                f"invalid_correlations={invalid_correlations}"
            )

        print("WEATHER_PRICE_ROWS:", mart_rows, flush=True)
        print("WEATHER_PRICE_DUPLICATES:", duplicates, flush=True)
        print("WEATHER_COVERAGE_ROWS:", weather_coverage, flush=True)
        print("WEATHER_PRICE_VALIDATION_PASSED", flush=True)

        mart.select(
            "market_date",
            "province",
            "source_family",
            "coffee_price_vnd_kg",
            "price_change_pct",
            "temperature_mean_c",
            "rain_sum_mm",
            "weather_station_count",
            "rain_price_corr30",
            "temperature_price_corr30"
        ).orderBy(
            F.col("market_date").desc(),
            "province"
        ).show(15, truncate=False)

        if "--write" in sys.argv[1:]:
            if spark.catalog.tableExists(TARGET):
                raise RuntimeError(
                    "Target already exists: " + TARGET
                )

            mart.writeTo(TARGET).using("iceberg").create()

            actual = spark.table(TARGET).count()
            if actual != mart_rows:
                raise RuntimeError("Iceberg row count mismatch")

            print("WEATHER_ICEBERG_ROWS:", actual, flush=True)
            print("WEATHER_ICEBERG_WRITE_PASSED", flush=True)
        else:
            print("WEATHER_PRICE_DRY_RUN_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
