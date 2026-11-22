"""
Build Gold monthly coffee trade Data Mart.

Source:
  lakehouse.gold.fact_coffee_trade_monthly

Dimensions:
  lakehouse.gold.dim_date
  lakehouse.gold.dim_destination
  lakehouse.gold.dim_geography
  lakehouse.gold.dim_source

Target:
  lakehouse.gold.mart_coffee_trade_monthly

Default mode: dry run only.
Write to Iceberg only when --write is supplied.

Grain:
  One row per year + month + market_code.
  ALL_MARKETS is the national total and must not be added to
  the individual destination rows.
"""

import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


FACT = "lakehouse.gold.fact_coffee_trade_monthly"
DIM_DATE = "lakehouse.gold.dim_date"
DIM_DESTINATION = "lakehouse.gold.dim_destination"
DIM_GEOGRAPHY = "lakehouse.gold.dim_geography"
DIM_SOURCE = "lakehouse.gold.dim_source"

TARGET = "lakehouse.gold.mart_coffee_trade_monthly"
WRITE = "--write" in sys.argv[1:]


def create_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Build-Mart-Coffee-Trade-Monthly")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def require_table(spark, table_name):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError("Required table does not exist: " + table_name)


def assert_unique(df, keys, label):
    total = df.count()
    unique = df.select(*keys).distinct().count()

    print(
        f"{label}_ROWS={total}; {label}_UNIQUE_KEYS={unique}",
        flush=True,
    )

    if total != unique:
        (
            df.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
            .show(20, truncate=False)
        )
        raise RuntimeError(f"{label}: duplicate key {keys}")


def build_base(spark):
    for table_name in (
        FACT,
        DIM_DATE,
        DIM_DESTINATION,
        DIM_GEOGRAPHY,
        DIM_SOURCE,
    ):
        require_table(spark, table_name)

    fact = spark.table(FACT)
    dates = spark.table(DIM_DATE)
    destinations = spark.table(DIM_DESTINATION)
    geography = spark.table(DIM_GEOGRAPHY)
    sources = spark.table(DIM_SOURCE)

    required_fact_columns = [
        "trade_fact_key",
        "date_key",
        "destination_key",
        "source_key",
        "geography_key",
        "year",
        "month",
        "market_code",
        "market_type",
        "quantity_tonnes",
        "value_usd_thousand",
        "quality_status",
        "bronze_record_id",
        "object_key",
        "record_position",
        "silver_observation_id",
    ]

    missing = sorted(set(required_fact_columns) - set(fact.columns))
    if missing:
        raise RuntimeError(f"Fact missing required columns: {missing}")

    required_dimensions = [
        (dates, "date_key", "DIM_DATE"),
        (destinations, "destination_key", "DIM_DESTINATION"),
        (geography, "geography_key", "DIM_GEOGRAPHY"),
        (sources, "source_key", "DIM_SOURCE"),
    ]

    for df, key, label in required_dimensions:
        if key not in df.columns:
            raise RuntimeError(f"{label} missing key column {key}")
        assert_unique(df, [key], label)

        if df.filter(F.col(key).isNull()).limit(1).count():
            raise RuntimeError(f"{label} contains NULL {key}")

    # The monthly Fact grain is one row per year/month/market.
    fact_rows = fact.count()
    assert_unique(fact, ["trade_fact_key"], "FACT_TRADE_MONTHLY")
    assert_unique(
        fact,
        ["year", "month", "market_code"],
        "FACT_TRADE_MONTHLY_NATURAL_GRAIN",
    )

    joined = (
        fact.alias("f")
        .join(
            dates.select(
                "date_key", "full_date", "year", "month"
            ).alias("d"),
            F.col("f.date_key") == F.col("d.date_key"),
            "left",
        )
        .join(
            destinations.select(
                "destination_key", "partner_code", "partner_name"
            ).alias("dst"),
            F.col("f.destination_key") == F.col("dst.destination_key"),
            "left",
        )
        .join(
            geography.select(
                "geography_key",
                "geography_code",
                "geography_name",
                "geography_level",
                "region_name",
            ).alias("g"),
            F.col("f.geography_key") == F.col("g.geography_key"),
            "left",
        )
        .join(
            sources.select(
                "source_key", "source_code", "source_name"
            ).alias("src"),
            F.col("f.source_key") == F.col("src.source_key"),
            "left",
        )
        .select(
            F.col("f.trade_fact_key"),
            F.col("f.date_key"),
            F.col("d.full_date").alias("month_start_date"),
            F.col("f.year").alias("trade_year"),
            F.col("f.month").alias("trade_month"),
            F.date_format(F.col("d.full_date"), "yyyy-MM").alias(
                "calendar_month"
            ),
            F.quarter(F.col("d.full_date")).alias("trade_quarter"),
            F.col("f.destination_key"),
            F.col("dst.partner_code").alias("destination_code"),
            F.when(
                F.col("f.market_type") == "national_total",
                F.lit("Tổng tất cả thị trường"),
            )
            .otherwise(F.col("dst.partner_name"))
            .alias("market_name"),
            F.col("f.source_key"),
            F.col("src.source_code"),
            F.col("src.source_name"),
            F.col("f.geography_key"),
            F.col("g.geography_code"),
            F.col("g.geography_name"),
            F.col("g.geography_level"),
            F.col("g.region_name"),
            F.col("f.market_code"),
            F.col("f.market_type"),
            (F.col("f.market_type") == "national_total").alias(
                "is_national_total"
            ),
            F.col("f.quantity_tonnes"),
            F.col("f.value_usd_thousand"),
            F.col("f.quality_status"),
            F.col("f.bronze_record_id"),
            F.col("f.object_key"),
            F.col("f.record_position"),
            F.col("f.silver_observation_id"),
            F.col("d.year").alias("dimension_year"),
            F.col("d.month").alias("dimension_month"),
        )
    )

    joined_rows = joined.count()
    if joined_rows != fact_rows:
        raise RuntimeError(
            f"Dimension JOIN changed row count: "
            f"Fact={fact_rows}, joined={joined_rows}"
        )

    missing_dimensions = joined.filter(
        F.col("month_start_date").isNull()
        | F.col("destination_code").isNull()
        | F.col("market_name").isNull()
        | F.col("source_code").isNull()
        | F.col("geography_code").isNull()
    ).count()

    if missing_dimensions:
        raise RuntimeError(
            f"Missing Dimension references: {missing_dimensions}"
        )

    date_mismatches = joined.filter(
        (F.col("trade_year") != F.col("dimension_year"))
        | (F.col("trade_month") != F.col("dimension_month"))
        | (F.dayofmonth("month_start_date") != 1)
    ).count()

    if date_mismatches:
        raise RuntimeError(
            f"Fact/Date Dimension year-month mismatch: {date_mismatches}"
        )

    invalid_market_types = joined.filter(
        ~F.col("market_type").isin(
            "national_total", "country", "regional_group"
        )
    ).count()

    if invalid_market_types:
        raise RuntimeError(
            f"Unsupported market_type rows: {invalid_market_types}"
        )

    invalid_measurements = joined.filter(
        (F.col("quantity_tonnes") < 0)
        | (F.col("value_usd_thousand") < 0)
        | (
            F.col("quantity_tonnes").isNull()
            & F.col("value_usd_thousand").isNull()
        )
    ).count()

    if invalid_measurements:
        raise RuntimeError(
            f"Invalid or empty trade measurements: {invalid_measurements}"
        )

    invalid_quality = joined.filter(
        ~F.col("quality_status").isin("complete", "partial")
    ).count()

    if invalid_quality:
        raise RuntimeError(
            f"Unexpected quality_status values: {invalid_quality}"
        )

    quality_inconsistency = joined.filter(
        (
            (F.col("quality_status") == "complete")
            & (
                F.col("quantity_tonnes").isNull()
                | F.col("value_usd_thousand").isNull()
            )
        )
        | (
            (F.col("quality_status") == "partial")
            & F.col("quantity_tonnes").isNotNull()
            & F.col("value_usd_thousand").isNotNull()
        )
    ).count()

    if quality_inconsistency:
        raise RuntimeError(
            f"Quality status/measurement mismatch: {quality_inconsistency}"
        )

    base = (
        joined
        .drop("dimension_year", "dimension_month")
        .withColumn(
            "average_export_price_usd_per_tonne",
            F.when(
                F.col("quantity_tonnes") > 0,
                F.round(
                    F.col("value_usd_thousand") * F.lit(1000)
                    / F.col("quantity_tonnes"),
                    3,
                ),
            ),
        )
        .withColumn(
            "has_quantity",
            F.col("quantity_tonnes").isNotNull(),
        )
        .withColumn(
            "has_value",
            F.col("value_usd_thousand").isNotNull(),
        )
        .withColumn(
            "is_complete_measurement",
            F.col("quantity_tonnes").isNotNull()
            & F.col("value_usd_thousand").isNotNull(),
        )
        .withColumn(
            "is_partial_measurement",
            F.col("quantity_tonnes").isNull()
            | F.col("value_usd_thousand").isNull(),
        )
    )

    print("TRADE_FACT_ROWS:", fact_rows, flush=True)
    print("TRADE_BASE_ROWS:", base.count(), flush=True)
    print("TRADE_BASE_DIMENSION_VALIDATION_PASSED", flush=True)

    return base


def add_period_comparisons(base):
    # Exact previous calendar month and exact same month one year earlier.
    # This avoids treating a missing month as the previous period.
    current = base.alias("c")
    previous_month = base.alias("pm")
    previous_year = base.alias("py")

    same_market_month = (
        (F.col("c.market_code") == F.col("pm.market_code"))
        & (F.col("c.market_type") == F.col("pm.market_type"))
        & (F.col("c.geography_key") == F.col("pm.geography_key"))
        & (F.col("c.source_key") == F.col("pm.source_key"))
        & (
            F.col("pm.month_start_date")
            == F.add_months(F.col("c.month_start_date"), -1)
        )
    )

    same_market_year = (
        (F.col("c.market_code") == F.col("py.market_code"))
        & (F.col("c.market_type") == F.col("py.market_type"))
        & (F.col("c.geography_key") == F.col("py.geography_key"))
        & (F.col("c.source_key") == F.col("py.source_key"))
        & (
            F.col("py.month_start_date")
            == F.add_months(F.col("c.month_start_date"), -12)
        )
    )

    selected_current_columns = [
        F.col("c." + column).alias(column)
        for column in base.columns
    ]

    compared = (
        current
        .join(previous_month, same_market_month, "left")
        .join(previous_year, same_market_year, "left")
        .select(
            *selected_current_columns,
            F.col("pm.quantity_tonnes").alias(
                "previous_month_quantity_tonnes"
            ),
            F.col("pm.value_usd_thousand").alias(
                "previous_month_value_usd_thousand"
            ),
            F.col("pm.average_export_price_usd_per_tonne").alias(
                "previous_month_average_price_usd_per_tonne"
            ),
            F.col("py.quantity_tonnes").alias(
                "previous_year_quantity_tonnes"
            ),
            F.col("py.value_usd_thousand").alias(
                "previous_year_value_usd_thousand"
            ),
            F.col("py.average_export_price_usd_per_tonne").alias(
                "previous_year_average_price_usd_per_tonne"
            ),
        )
    )

    result = (
        compared
        .withColumn(
            "export_value_mom_pct",
            F.when(
                F.col("previous_month_value_usd_thousand") > 0,
                F.round(
                    (
                        F.col("value_usd_thousand")
                        / F.col("previous_month_value_usd_thousand")
                        - 1
                    ) * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "export_quantity_mom_pct",
            F.when(
                F.col("previous_month_quantity_tonnes") > 0,
                F.round(
                    (
                        F.col("quantity_tonnes")
                        / F.col("previous_month_quantity_tonnes")
                        - 1
                    ) * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "average_price_mom_pct",
            F.when(
                F.col("previous_month_average_price_usd_per_tonne") > 0,
                F.round(
                    (
                        F.col("average_export_price_usd_per_tonne")
                        / F.col("previous_month_average_price_usd_per_tonne")
                        - 1
                    ) * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "export_value_yoy_pct",
            F.when(
                F.col("previous_year_value_usd_thousand") > 0,
                F.round(
                    (
                        F.col("value_usd_thousand")
                        / F.col("previous_year_value_usd_thousand")
                        - 1
                    ) * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "export_quantity_yoy_pct",
            F.when(
                F.col("previous_year_quantity_tonnes") > 0,
                F.round(
                    (
                        F.col("quantity_tonnes")
                        / F.col("previous_year_quantity_tonnes")
                        - 1
                    ) * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "average_price_yoy_pct",
            F.when(
                F.col("previous_year_average_price_usd_per_tonne") > 0,
                F.round(
                    (
                        F.col("average_export_price_usd_per_tonne")
                        / F.col("previous_year_average_price_usd_per_tonne")
                        - 1
                    ) * 100,
                    3,
                ),
            ),
        )
    )

    return result


def add_national_share_and_ranking(mart):
    national = (
        mart.filter(F.col("is_national_total"))
        .select(
            "month_start_date",
            "geography_key",
            F.col("value_usd_thousand").alias(
                "national_export_value_usd_thousand"
            ),
            F.col("quantity_tonnes").alias(
                "national_export_quantity_tonnes"
            ),
        )
    )

    result = (
        mart.join(
            national,
            ["month_start_date", "geography_key"],
            "left",
        )
        .withColumn(
            "destination_value_share_of_national_pct",
            F.when(
                F.col("is_national_total")
                & (F.col("national_export_value_usd_thousand") > 0),
                F.lit(100.0),
            )
            .when(
                (~F.col("is_national_total"))
                & (F.col("national_export_value_usd_thousand") > 0),
                F.round(
                    F.col("value_usd_thousand")
                    / F.col("national_export_value_usd_thousand")
                    * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "destination_quantity_share_of_national_pct",
            F.when(
                F.col("is_national_total")
                & (F.col("national_export_quantity_tonnes") > 0),
                F.lit(100.0),
            )
            .when(
                (~F.col("is_national_total"))
                & (F.col("national_export_quantity_tonnes") > 0),
                F.round(
                    F.col("quantity_tonnes")
                    / F.col("national_export_quantity_tonnes")
                    * 100,
                    3,
                ),
            ),
        )
    )

    rank_window = (
        Window.partitionBy("month_start_date", "market_type")
        .orderBy(F.col("value_usd_thousand").desc_nulls_last())
    )

    result = result.withColumn(
        "destination_rank_by_value",
        F.when(
            ~F.col("is_national_total"),
            F.dense_rank().over(rank_window),
        ),
    )

    return result


def validate_mart(base, mart):
    fact_rows = base.count()
    mart_rows = mart.count()

    if mart_rows != fact_rows:
        raise RuntimeError(
            f"Mart row count changed: base={fact_rows}, mart={mart_rows}"
        )

    assert_unique(mart, ["trade_fact_key"], "MART_TRADE_MONTHLY")
    assert_unique(
        mart,
        ["trade_year", "trade_month", "market_code"],
        "MART_TRADE_MONTHLY_NATURAL_GRAIN",
    )

    invalid_price = mart.filter(
        F.col("average_export_price_usd_per_tonne") < 0
    ).count()

    invalid_share = mart.filter(
        (F.col("destination_value_share_of_national_pct") < 0)
        | (F.col("destination_quantity_share_of_national_pct") < 0)
    ).count()

    # Shares above 100% are reported for investigation rather than
    # silently discarded; overlapping regional groups can be non-additive.
    share_over_100 = mart.filter(
        (F.col("destination_value_share_of_national_pct") > 100)
        | (F.col("destination_quantity_share_of_national_pct") > 100)
    ).count()

    if invalid_price or invalid_share:
        raise RuntimeError(
            f"Invalid KPI values: price={invalid_price}, share={invalid_share}"
        )

    print("TRADE_MART_ROWS:", mart_rows, flush=True)
    print("TRADE_MART_DUPLICATE_KEYS: 0", flush=True)
    print("TRADE_MART_INVALID_PRICE_KPI:", invalid_price, flush=True)
    print("TRADE_MART_INVALID_SHARE_KPI:", invalid_share, flush=True)
    print("TRADE_MART_SHARE_OVER_100_WARNING:", share_over_100, flush=True)

    print("TRADE_MART_QUALITY_STATUS:", flush=True)
    mart.groupBy("quality_status").count().orderBy("quality_status").show(
        truncate=False
    )

    print("TRADE_MART_MARKET_TYPE:", flush=True)
    mart.groupBy("market_type").count().orderBy("market_type").show(
        truncate=False
    )

    print("TRADE_MART_VALIDATION_PASSED", flush=True)


def main():
    spark = create_spark()

    try:
        base = build_base(spark)
        mart = add_period_comparisons(base)
        mart = add_national_share_and_ranking(mart)

        validate_mart(base, mart)

        print("TRADE_MART_SAMPLE:", flush=True)
        mart.select(
            "calendar_month",
            "market_code",
            "market_name",
            "market_type",
            "quantity_tonnes",
            "value_usd_thousand",
            "average_export_price_usd_per_tonne",
            "export_value_mom_pct",
            "export_value_yoy_pct",
            "destination_value_share_of_national_pct",
            "destination_rank_by_value",
            "quality_status",
        ).orderBy(
            F.col("month_start_date").desc(),
            F.col("market_type"),
            F.col("destination_rank_by_value").asc_nulls_last(),
        ).show(30, truncate=False)

        if WRITE:
            if spark.catalog.tableExists(TARGET):
                raise RuntimeError(
                    "Target already exists; refusing to overwrite: " + TARGET
                )

            print("TRADE_MART_ICEBERG_WRITE_START:", TARGET, flush=True)
            mart.writeTo(TARGET).using("iceberg").create()

            saved = spark.table(TARGET)
            saved_rows = saved.count()
            saved_unique = saved.select("trade_fact_key").distinct().count()

            if saved_rows != mart.count() or saved_unique != saved_rows:
                raise RuntimeError(
                    "Iceberg write verification failed: "
                    f"rows={saved_rows}, unique_keys={saved_unique}"
                )

            print("TRADE_MART_ICEBERG_ROWS:", saved_rows, flush=True)
            print("TRADE_MART_ICEBERG_WRITE_PASSED", flush=True)
        else:
            print("TRADE_MART_DRY_RUN_PASSED", flush=True)
            print("NO_GOLD_MART_WRITTEN", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
