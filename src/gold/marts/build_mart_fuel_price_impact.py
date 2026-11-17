import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "lakehouse.gold.mart_fuel_price_impact"


def main():
    spark = (
        SparkSession.builder
        .appName("Build-Mart-Fuel-Price-Impact")
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
        domestic = spark.table(
            "lakehouse.gold.mart_coffee_price_trends"
        )
        fuel = spark.table(
            "lakehouse.gold.fact_fuel_price"
        )
        dates = spark.table(
            "lakehouse.gold.dim_date"
        )
        series = spark.table(
            "lakehouse.gold.dim_fuel_series"
        )

        # 1. Validate keys.
        for name, df, key in [
            ("dim_date", dates, "date_key"),
            ("dim_fuel_series", series, "fuel_series_key"),
        ]:
            if df.count() != df.select(key).distinct().count():
                raise RuntimeError(
                    "Duplicate Dimension key: " + name
                )

        # 2. Domestic coffee series.
        prices = domestic.select(
            F.col("market_date"),
            F.col("province"),
            F.col("price_series_key"),
            F.col("source_family").alias(
                "coffee_source_family"
            ),
            F.col("coffee_price_vnd_kg"),
            F.col("price_change_pct").alias(
                "coffee_change_pct"
            ),
            F.col("quality_status").alias(
                "coffee_quality_status"
            )
        )

        # 3. Fuel observations with their own metadata.
        fuel_base = (
            fuel.alias("f")
            .join(
                dates.alias("d"),
                F.col("f.date_key") == F.col("d.date_key")
            )
            .join(
                series.alias("s"),
                F.col("f.fuel_series_key")
                == F.col("s.fuel_series_key")
            )
            .select(
                F.col("f.fuel_fact_key"),
                F.col("d.full_date").alias(
                    "fuel_effective_date"
                ),
                F.col("f.fuel_series_key"),
                F.col("s.series_code").alias(
                    "fuel_series_code"
                ),
                F.col("s.source_family").alias(
                    "fuel_source_family"
                ),
                F.col("s.region").alias("fuel_region"),
                F.col("s.unit").alias("fuel_unit"),
                F.col("f.price_vnd_per_unit").alias(
                    "fuel_price_vnd_per_unit"
                ),
                F.col("f.price_change_percent").alias(
                    "fuel_change_pct"
                ),
                F.col("f.price_jump_review"),
                F.col("f.quality_status").alias(
                    "fuel_quality_status"
                )
            )
        )

        # One observation per date and fuel series.
        fuel_duplicates = (
            fuel_base.groupBy(
                "fuel_effective_date",
                "fuel_series_key"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if fuel_duplicates:
            raise RuntimeError(
                "Duplicate fuel date/series keys"
            )

        # 4. Construct valid-date intervals.
        # Each fuel price remains applicable until
        # the next observation in the same series.
        fuel_window = (
            Window.partitionBy("fuel_series_key")
            .orderBy("fuel_effective_date")
        )

        fuel_intervals = fuel_base.withColumn(
            "next_fuel_date",
            F.lead("fuel_effective_date").over(
                fuel_window
            )
        )

        # 5. As-of join.
        # Preserve each fuel series separately.
        # No inferred mapping from fuel region to province.
        mart = (
            prices.alias("p")
            .join(
                fuel_intervals.alias("f"),
                (
                    F.col("p.market_date")
                    >= F.col("f.fuel_effective_date")
                )
                & (
                    F.col("f.next_fuel_date").isNull()
                    | (
                        F.col("p.market_date")
                        < F.col("f.next_fuel_date")
                    )
                ),
                "inner"
            )
            .select(
                F.col("p.market_date"),
                F.col("p.province"),
                F.col("p.price_series_key"),
                F.col("p.coffee_source_family"),
                F.col("p.coffee_price_vnd_kg"),
                F.col("p.coffee_change_pct"),
                F.col("p.coffee_quality_status"),
                F.col("f.fuel_series_key"),
                F.col("f.fuel_series_code"),
                F.col("f.fuel_source_family"),
                F.col("f.fuel_region"),
                F.col("f.fuel_unit"),
                F.col("f.fuel_effective_date"),
                F.col("f.fuel_price_vnd_per_unit"),
                F.col("f.fuel_change_pct"),
                F.col("f.price_jump_review"),
                F.col("f.fuel_quality_status")
            )
            .withColumn(
                "fuel_age_days",
                F.datediff(
                    F.col("market_date"),
                    F.col("fuel_effective_date")
                )
            )
            .withColumn(
                "is_fuel_update_day",
                F.col("market_date")
                == F.col("fuel_effective_date")
            )
            .withColumn(
                "change_gap_percentage_points",
                F.when(
                    F.col("is_fuel_update_day")
                    & F.col("coffee_change_pct").isNotNull()
                    & F.col("fuel_change_pct").isNotNull(),
                    F.round(
                        F.col("coffee_change_pct")
                        - F.col("fuel_change_pct"),
                        3
                    )
                )
            )
        )

        # 6. Validate resulting Data Mart.
        mart_rows = mart.count()

        duplicate_keys = (
            mart.groupBy(
                "market_date",
                "province",
                "price_series_key",
                "fuel_series_key"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid_prices = mart.filter(
            F.col("coffee_price_vnd_kg").isNull()
            | (F.col("coffee_price_vnd_kg") <= 0)
            | F.col("fuel_price_vnd_per_unit").isNull()
            | (F.col("fuel_price_vnd_per_unit") <= 0)
        ).count()

        invalid_dates = mart.filter(
            F.col("fuel_age_days") < 0
        ).count()

        fuel_series_count = (
            mart.select("fuel_series_key")
            .distinct()
            .count()
        )

        print(
            "FUEL_MART_ROWS:", mart_rows,
            flush=True
        )
        print(
            "FUEL_SERIES_COUNT:", fuel_series_count,
            flush=True
        )
        print(
            "FUEL_DUPLICATES:", duplicate_keys,
            flush=True
        )
        print(
            "FUEL_INVALID_PRICES:", invalid_prices,
            flush=True
        )
        print(
            "FUEL_INVALID_DATES:", invalid_dates,
            flush=True
        )

        if (
            mart_rows == 0
            or duplicate_keys != 0
            or invalid_prices != 0
            or invalid_dates != 0
        ):
            raise RuntimeError(
                "Fuel Data Mart validation failed"
            )

        print(
            "FUEL_MART_VALIDATION_PASSED",
            flush=True
        )

        mart.select(
            "market_date",
            "province",
            "coffee_source_family",
            "coffee_price_vnd_kg",
            "fuel_series_code",
            "fuel_region",
            "fuel_unit",
            "fuel_price_vnd_per_unit",
            "fuel_effective_date",
            "fuel_age_days"
        ).orderBy(
            F.col("market_date").desc(),
            "province",
            "fuel_series_code"
        ).show(15, truncate=False)

        # 7. Explicit write mode.
        if "--write" in sys.argv[1:]:
            if spark.catalog.tableExists(TARGET):
                raise RuntimeError(
                    "Target already exists: " + TARGET
                )

            mart.writeTo(TARGET).using(
                "iceberg"
            ).create()

            actual = spark.table(TARGET).count()

            if actual != mart_rows:
                raise RuntimeError(
                    "Iceberg row count mismatch"
                )

            print(
                "FUEL_ICEBERG_ROWS:", actual,
                flush=True
            )
            print(
                "FUEL_ICEBERG_WRITE_PASSED",
                flush=True
            )
        else:
            print(
                "FUEL_MART_DRY_RUN_PASSED",
                flush=True
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
