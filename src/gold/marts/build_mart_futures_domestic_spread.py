import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "lakehouse.gold.mart_futures_domestic_spread"


def main():
    spark = (
        SparkSession.builder
        .appName("Build-Mart-Futures-Domestic-Spread")
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
            "lakehouse.gold.fact_domestic_price"
        )
        futures = spark.table(
            "lakehouse.gold.fact_futures_price"
        )
        dates = spark.table("lakehouse.gold.dim_date")
        geography = spark.table("lakehouse.gold.dim_geography")
        series = spark.table("lakehouse.gold.dim_price_series")

        for name, df, key in [
            ("dim_date", dates, "date_key"),
            ("dim_geography", geography, "geography_key"),
            ("dim_price_series", series, "price_series_key"),
        ]:
            if df.count() != df.select(key).distinct().count():
                raise RuntimeError("Duplicate key in " + name)

        # 1. Domestic prices: date x province x series.
        d = (
            domestic.alias("f")
            .join(
                dates.alias("dt"),
                F.col("f.date_key") == F.col("dt.date_key")
            )
            .join(
                geography.alias("g"),
                F.col("f.geography_key")
                == F.col("g.geography_key")
            )
            .join(
                series.alias("s"),
                F.col("f.price_series_key")
                == F.col("s.price_series_key")
            )
            .select(
                F.col("dt.full_date").alias("market_date"),
                F.col("g.geography_code").alias("province"),
                F.col("s.price_series_key"),
                F.col("s.source_family"),
                F.col("f.price_vnd_kg").alias(
                    "domestic_price_vnd_kg"
                ),
                F.col("f.quality_status").alias(
                    "domestic_quality_status"
                )
            )
        )

        # 2. Futures: aggregate to one row per trading date.
        # Do not assume multiple Fact rows on one date
        # represent the same contract or market series.
        f_daily = (
            futures.alias("f")
            .join(
                dates.alias("dt"),
                F.col("f.trade_date_key") == F.col("dt.date_key")
            )
            .groupBy(
                F.col("dt.full_date").alias("market_date")
            )
            .agg(
                F.round(F.avg("f.last_price"), 2).alias(
                    "futures_avg_last_price"
                ),
                F.round(F.avg("f.change_percent"), 3).alias(
                    "futures_avg_change_pct"
                ),
                F.count("*").alias("futures_observation_count"),
                F.count("f.last_price").alias(
                    "futures_valid_price_count"
                ),
                F.sum(
                    F.when(F.col("f.ohlc_review") == True, 1)
                    .otherwise(0)
                ).alias("futures_ohlc_review_count")
            )
        )

        # 3. Domestic change per individual price series.
        w = (
            Window.partitionBy("price_series_key")
            .orderBy("market_date")
        )

        domestic_kpi = (
            d.withColumn(
                "previous_domestic_price",
                F.lag("domestic_price_vnd_kg").over(w)
            )
            .withColumn(
                "domestic_change_pct",
                F.when(
                    F.col("previous_domestic_price") > 0,
                    F.round(
                        (
                            F.col("domestic_price_vnd_kg")
                            / F.col("previous_domestic_price") - 1
                        ) * 100,
                        3
                    )
                )
            )
            .drop("previous_domestic_price")
        )

        # 4. Join by matching dates.
        mart = (
            domestic_kpi
            .join(f_daily, "market_date", "inner")
            .withColumn(
                "change_gap_percentage_points",
                F.when(
                    F.col("domestic_change_pct").isNotNull()
                    & F.col("futures_avg_change_pct").isNotNull(),
                    F.round(
                        F.col("domestic_change_pct")
                        - F.col("futures_avg_change_pct"),
                        3
                    )
                )
            )
        )

        # 5. Quality checks.
        rows = mart.count()

        duplicates = (
            mart.groupBy(
                "market_date", "province", "price_series_key"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid = mart.filter(
            F.col("domestic_price_vnd_kg").isNull()
            | (F.col("domestic_price_vnd_kg") <= 0)
        ).count()

        if rows == 0 or duplicates != 0 or invalid != 0:
            raise RuntimeError(
                f"Invalid mart: rows={rows}, "
                f"duplicates={duplicates}, invalid={invalid}"
            )

        print("SPREAD_MART_ROWS:", rows, flush=True)
        print("SPREAD_DUPLICATES:", duplicates, flush=True)
        print("SPREAD_INVALID_PRICES:", invalid, flush=True)

        mart.orderBy(F.col("market_date").desc()).show(
            10, truncate=False
        )

        print("SPREAD_MART_VALIDATION_PASSED", flush=True)

        # 6. Write only with explicit --write.
        if "--write" in sys.argv[1:]:
            if spark.catalog.tableExists(TARGET):
                raise RuntimeError(
                    "Target already exists: " + TARGET
                )

            mart.writeTo(TARGET).using("iceberg").create()

            actual = spark.table(TARGET).count()
            if actual != rows:
                raise RuntimeError("Iceberg row count mismatch")

            print("SPREAD_ICEBERG_ROWS:", actual, flush=True)
            print("SPREAD_ICEBERG_WRITE_PASSED", flush=True)
        else:
            print("SPREAD_MART_DRY_RUN_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
