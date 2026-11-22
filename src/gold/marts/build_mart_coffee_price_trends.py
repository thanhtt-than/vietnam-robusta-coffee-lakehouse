import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_FACT = "lakehouse.gold.fact_domestic_price"
DIM_DATE = "lakehouse.gold.dim_date"
DIM_GEOGRAPHY = "lakehouse.gold.dim_geography"
DIM_PRICE_SERIES = "lakehouse.gold.dim_price_series"

TARGET = "lakehouse.gold.mart_coffee_price_trends"

# Start a new price-history segment when consecutive observations are
# separated by more than 30 calendar days. This prevents rolling KPIs
# from mixing observations across long periods with no source data.
MAX_GAP_DAYS = 30

def get_spark():
    return (
        SparkSession.builder
        .appName("Build-Mart-Coffee-Price-Trends")
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

if __name__ == "__main__":
    print("PRICE_TRENDS_SCRIPT_READY")
    print("SOURCE_FACT:", SOURCE_FACT)
    print("TARGET:", TARGET)
    print("MAX_GAP_DAYS:", MAX_GAP_DAYS)

def load_sources(spark):
    tables = {
        "fact": SOURCE_FACT,
        "date": DIM_DATE,
        "geography": DIM_GEOGRAPHY,
        "series": DIM_PRICE_SERIES,
    }

    missing = [
        name for name in tables.values()
        if not spark.catalog.tableExists(name)
    ]

    if missing:
        print("SOURCE_TABLES_NOT_READY:", ", ".join(missing))
        return None

    sources = {
        key: spark.table(name)
        for key, name in tables.items()
    }

    print("SOURCE_TABLES_READY")

    for key, df in sources.items():
        print(f"{key.upper()}_ROWS:", df.count())

    return sources

def build_price_base(sources):
    fact = sources["fact"]
    dates = sources["date"]
    geography = sources["geography"]
    series = sources["series"]

    # Validate dimension keys before joining.
    for name, df, key in [
        ("dim_date", dates, "date_key"),
        ("dim_geography", geography, "geography_key"),
        ("dim_price_series", series, "price_series_key"),
    ]:
        total = df.count()
        distinct = df.select(key).distinct().count()

        if total != distinct:
            raise RuntimeError(
                f"Duplicate dimension key: {name}.{key}"
            )

    base = (
        fact.alias("f")
        .join(
            dates.alias("d"),
            F.col("f.date_key") == F.col("d.date_key"),
            "left"
        )
        .join(
            geography.alias("g"),
            F.col("f.geography_key") == F.col("g.geography_key"),
            "left"
        )
        .join(
            series.alias("s"),
            F.col("f.price_series_key") == F.col("s.price_series_key"),
            "left"
        )
        .select(
            F.col("f.price_fact_key"),
            F.col("d.full_date").alias("market_date"),
            F.col("g.geography_code").alias("province"),
            F.col("s.price_series_key"),
            F.col("s.source_family"),
            F.col("s.series_code"),
            F.col("f.price_vnd_kg").alias("coffee_price_vnd_kg"),
            F.col("f.quality_status"),
            F.col("f.raw_observation_count"),
            F.col("f.distinct_price_count"),
        )
    )

    total = fact.count()
    result = base.count()

    if total != result:
        raise RuntimeError(
            f"JOIN row count mismatch: fact={total}, joined={result}"
        )

    if base.filter(
        F.col("market_date").isNull()
        | F.col("province").isNull()
        | F.col("price_series_key").isNull()
    ).count() > 0:
        raise RuntimeError("Missing Dimension references")

    if base.select(
        "market_date", "province", "price_series_key"
    ).distinct().count() != result:
        raise RuntimeError("Duplicate price trend grain")

    print("PRICE_BASE_JOIN_PASSED")
    print("PRICE_BASE_ROWS:", result)

    return base

def calculate_price_kpis(base):
    # A price series is identified by province + price_series_key.
    # The extra province partition is deliberate protection against
    # accidental cross-province mixing if source keys are reused later.
    series_partition = ["province", "price_series_key"]
    ordered_window = (
        Window.partitionBy(*series_partition)
        .orderBy("market_date")
    )

    staged = (
        base
        .withColumn("_previous_date", F.lag("market_date").over(ordered_window))
        .withColumn(
            "_previous_price",
            F.lag("coffee_price_vnd_kg").over(ordered_window)
        )
        .withColumn(
            "days_since_previous_observation",
            F.datediff(F.col("market_date"), F.col("_previous_date"))
        )
        .withColumn(
            "_new_segment_flag",
            F.when(
                F.col("_previous_date").isNull()
                | (F.col("days_since_previous_observation") > MAX_GAP_DAYS),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

    # Cumulative segment number. A gap > MAX_GAP_DAYS starts a fresh
    # segment, so moving averages and volatility cannot reach across it.
    segment_window = (
        Window.partitionBy(*series_partition)
        .orderBy("market_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    staged = staged.withColumn(
        "_price_segment_id",
        F.sum("_new_segment_flag").over(segment_window)
    )

    rolling_order = (
        Window.partitionBy(*series_partition, "_price_segment_id")
        .orderBy("market_date")
    )
    w7 = rolling_order.rowsBetween(-6, 0)
    w30 = rolling_order.rowsBetween(-29, 0)

    result = (
        staged
        .withColumn(
            "is_new_price_segment",
            F.col("_new_segment_flag") == 1
        )
        # Do not compare a price with an observation from before a long gap.
        .withColumn(
            "previous_price_vnd_kg",
            F.when(
                F.col("days_since_previous_observation").isNotNull()
                & (F.col("days_since_previous_observation") <= MAX_GAP_DAYS),
                F.col("_previous_price")
            )
        )
        .withColumn(
            "price_change_vnd_kg",
            F.col("coffee_price_vnd_kg") - F.col("previous_price_vnd_kg")
        )
        .withColumn(
            "price_change_pct",
            F.when(
                F.col("previous_price_vnd_kg") > 0,
                F.round(
                    F.col("price_change_vnd_kg")
                    / F.col("previous_price_vnd_kg") * 100,
                    3
                )
            )
        )
        .withColumn(
            "ma7_observation_count",
            F.count("coffee_price_vnd_kg").over(w7)
        )
        .withColumn(
            "ma30_observation_count",
            F.count("coffee_price_vnd_kg").over(w30)
        )
        .withColumn(
            "ma7_vnd_kg",
            F.round(F.avg("coffee_price_vnd_kg").over(w7), 2)
        )
        .withColumn(
            "ma30_vnd_kg",
            F.round(F.avg("coffee_price_vnd_kg").over(w30), 2)
        )
        .withColumn(
            "min30_vnd_kg",
            F.min("coffee_price_vnd_kg").over(w30)
        )
        .withColumn(
            "max30_vnd_kg",
            F.max("coffee_price_vnd_kg").over(w30)
        )
        .withColumn(
            "volatility30_vnd_kg",
            F.round(F.stddev_samp("coffee_price_vnd_kg").over(w30), 2)
        )
        .drop(
            "_previous_date",
            "_previous_price",
            "_new_segment_flag",
            "_price_segment_id"
        )
    )

    print("PRICE_TRENDS_KPI_READY")
    print("PRICE_TRENDS_MAX_GAP_DAYS:", MAX_GAP_DAYS)
    return result

def validate_price_trends(base, mart):
    base_rows = base.count()
    mart_rows = mart.count()

    if base_rows != mart_rows:
        raise RuntimeError(
            f"Row count mismatch: base={base_rows}, mart={mart_rows}"
        )

    grain = ["market_date", "province", "price_series_key"]

    duplicate_keys = (
        mart.groupBy(*grain)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_keys != 0:
        raise RuntimeError(
            f"Duplicate Data Mart keys: {duplicate_keys}"
        )

    invalid_prices = mart.filter(
        F.col("coffee_price_vnd_kg").isNull()
        | (F.col("coffee_price_vnd_kg") <= 0)
    ).count()

    if invalid_prices != 0:
        raise RuntimeError(
            f"Invalid coffee prices: {invalid_prices}"
        )

    invalid_ma7 = mart.filter(
        F.col("ma7_vnd_kg").isNull()
        | (F.col("ma7_vnd_kg") <= 0)
        | (F.col("ma7_observation_count") < 1)
        | (F.col("ma7_observation_count") > 7)
    ).count()

    invalid_ma30 = mart.filter(
        F.col("ma30_vnd_kg").isNull()
        | (F.col("ma30_vnd_kg") <= 0)
        | (F.col("ma30_observation_count") < 1)
        | (F.col("ma30_observation_count") > 30)
    ).count()

    if invalid_ma7 or invalid_ma30:
        raise RuntimeError(
            f"Invalid moving averages: MA7={invalid_ma7}, MA30={invalid_ma30}"
        )

    invalid_range = mart.filter(
        (F.col("ma30_vnd_kg") < F.col("min30_vnd_kg"))
        | (F.col("ma30_vnd_kg") > F.col("max30_vnd_kg"))
    ).count()

    if invalid_range != 0:
        raise RuntimeError(
            f"MA30 outside price range: {invalid_range}"
        )

    # Every observation after a long gap must start a new segment:
    # previous-price return is NULL and both rolling windows contain
    # only the current observation at that first row.
    bad_gap_reset = mart.filter(
        (F.col("days_since_previous_observation") > MAX_GAP_DAYS)
        & (
            F.col("previous_price_vnd_kg").isNotNull()
            | (F.col("ma7_observation_count") != 1)
            | (F.col("ma30_observation_count") != 1)
            | (F.col("ma7_vnd_kg") != F.col("coffee_price_vnd_kg"))
            | (F.col("ma30_vnd_kg") != F.col("coffee_price_vnd_kg"))
            | (F.col("is_new_price_segment") != F.lit(True))
        )
    ).count()

    if bad_gap_reset != 0:
        raise RuntimeError(
            f"Rolling KPI crossed a long data gap: {bad_gap_reset}"
        )

    print("PRICE_TRENDS_ROWS:", mart_rows)
    print("PRICE_TRENDS_DUPLICATE_KEYS:", duplicate_keys)
    print("PRICE_TRENDS_INVALID_PRICES:", invalid_prices)
    print("PRICE_TRENDS_INVALID_MA7:", invalid_ma7)
    print("PRICE_TRENDS_INVALID_MA30:", invalid_ma30)
    print("PRICE_TRENDS_LONG_GAP_RESET_ERRORS:", bad_gap_reset)
    print("PRICE_TRENDS_VALIDATION_PASSED")

def main():
    parser = argparse.ArgumentParser(
        description="Build the gap-aware coffee price trends Data Mart."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the Data Mart to Iceberg. Without this flag, run DRY RUN.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Explicitly replace the existing target table. Requires --write.",
    )
    args = parser.parse_args()

    if args.replace_existing and not args.write:
        parser.error("--replace-existing requires --write")

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        sources = load_sources(spark)

        if sources is None:
            raise RuntimeError("Missing Fact/Dimension tables")

        base = build_price_base(sources)
        mart = calculate_price_kpis(base)

        validate_price_trends(base, mart)

        print("PRICE_TRENDS_SAMPLE:", flush=True)

        mart.select(
            "market_date",
            "province",
            "source_family",
            "coffee_price_vnd_kg",
            "days_since_previous_observation",
            "is_new_price_segment",
            "price_change_pct",
            "ma7_vnd_kg",
            "ma7_observation_count",
            "ma30_vnd_kg",
            "ma30_observation_count",
            "volatility30_vnd_kg"
        ).orderBy(
            F.col("market_date").desc(),
            "province"
        ).show(20, truncate=False)

        if args.write:
            write_price_trends(
                spark,
                mart,
                replace_existing=args.replace_existing
            )
        else:
            print("PRICE_TRENDS_DRY_RUN_PASSED", flush=True)

    finally:
        spark.stop()




def write_price_trends(spark, mart, replace_existing=False):
    target_exists = spark.catalog.tableExists(TARGET)

    if target_exists and not replace_existing:
        raise RuntimeError(
            f"Target already exists: {TARGET}. "
            "Run a DRY RUN first, then use --write --replace-existing "
            "only when you intentionally want to replace it."
        )

    writer = mart.writeTo(TARGET).using("iceberg")

    if target_exists:
        writer.replace()
        operation = "REPLACE"
    else:
        writer.create()
        operation = "CREATE"

    actual = spark.table(TARGET).count()
    expected = mart.count()

    if actual != expected:
        raise RuntimeError(
            f"Written rows mismatch: {actual} != {expected}"
        )

    print("PRICE_TRENDS_ICEBERG_ROWS:", actual, flush=True)
    print(f"PRICE_TRENDS_ICEBERG_{operation}_PASSED", flush=True)


if __name__ == "__main__":
    main()
