"""Gold Futures Fact: validated, append-only Iceberg load.

Default execution is read-only (dry run). Pass --write to append new
futures observations. Existing fact rows are never updated or deleted.
If an incoming key already exists with different business/lineage values,
the job stops instead of silently changing historical data.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TARGET = "lakehouse.gold.fact_futures_price"

COLUMNS = [
    "futures_fact_key",
    "trade_date_key",
    "source_key",
    "open_price",
    "high_price",
    "low_price",
    "last_price",
    "volume_normalized",
    "change_percent",
    "ohlc_review",
    "volume_missing",
    "volume_invalid",
    "change_missing",
    "raw_version_conflict",
    "quality_status",
    "raw_observation_count",
    "distinct_raw_version_count",
    "selected_bronze_record_id",
    "selected_bronze_object_key",
    "selected_bronze_record_position",
    "gold_processed_at",
]

BUSINESS_COLUMNS = [
    name for name in COLUMNS if name != "gold_processed_at"
]

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-Futures-Price")
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def require_columns(df, required, label):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise RuntimeError(f"{label} missing columns: {missing}")


def validate_dimension(df, name, natural_key, surrogate_key):
    total = df.count()
    unique_natural = df.select(natural_key).distinct().count()
    null_natural = df.filter(F.col(natural_key).isNull()).count()
    null_surrogate = df.filter(F.col(surrogate_key).isNull()).count()

    print(f"{name}_ROWS:", total, flush=True)
    print(f"{name}_UNIQUE_KEYS:", unique_natural, flush=True)
    print(f"{name}_NULL_NATURAL_KEYS:", null_natural, flush=True)
    print(f"{name}_NULL_SURROGATE_KEYS:", null_surrogate, flush=True)

    if (
        total == 0
        or total != unique_natural
        or null_natural != 0
        or null_surrogate != 0
    ):
        raise RuntimeError(f"Invalid or duplicate dimension keys: {name}")


def print_unmatched_values(df, column, label):
    values = [
        row[column]
        for row in df.select(column).distinct().limit(20).collect()
    ]
    print(f"{label}_EXAMPLES:", values, flush=True)


try:
    london = spark.table("lakehouse.silver.london_futures_daily")

    require_columns(
        london,
        [
            "trade_date",
            "selected_source",
            "open_price",
            "high_price",
            "low_price",
            "last_price",
            "volume_normalized",
            "change_percent",
            "ohlc_review",
            "volume_missing",
            "volume_invalid",
            "change_missing",
            "raw_version_conflict",
            "quality_status",
            "raw_observation_count",
            "distinct_raw_version_count",
            "selected_bronze_record_id",
            "selected_bronze_object_key",
            "selected_bronze_record_position",
        ],
        "Silver London Futures",
    )

    dates = spark.table(
        "lakehouse.gold.dim_date"
    ).select("date_key", "full_date")

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select("source_key", "source_code")

    print("=== INPUT VALIDATION ===", flush=True)

    input_total = london.count()
    unique_trade_dates = london.select("trade_date").distinct().count()
    null_trade_dates = london.filter(
        F.col("trade_date").isNull()
    ).count()

    print("FUTURES_SILVER_TOTAL:", input_total, flush=True)
    print("FUTURES_SILVER_UNIQUE_TRADE_DATES:", unique_trade_dates, flush=True)
    print("FUTURES_SILVER_NULL_TRADE_DATES:", null_trade_dates, flush=True)

    # Business grain remains one observation per trading date.
    # The total number of observations is intentionally not hard-coded.
    if (
        input_total <= 0
        or unique_trade_dates != input_total
        or null_trade_dates != 0
    ):
        raise RuntimeError(
            "Silver Futures must contain exactly one non-NULL row per trade_date"
        )

    validate_dimension(dates, "dim_date", "full_date", "date_key")
    validate_dimension(sources, "dim_source", "source_code", "source_key")

    print("=== JOIN DIMENSIONS ===", flush=True)

    joined = (
        london.alias("f")
        .join(
            dates.alias("d"),
            F.col("f.trade_date") == F.col("d.full_date"),
            "left",
        )
        .join(
            sources.alias("s"),
            F.col("f.selected_source") == F.col("s.source_code"),
            "left",
        )
    )

    joined_total = joined.count()
    print("FUTURES_ROWS_AFTER_DIMENSION_JOIN:", joined_total, flush=True)

    missing_dates = joined.filter(F.col("date_key").isNull()).count()
    missing_sources = joined.filter(F.col("source_key").isNull()).count()

    print("UNMATCHED_DATE_KEYS:", missing_dates, flush=True)
    print("UNMATCHED_SOURCE_KEYS:", missing_sources, flush=True)

    if missing_dates:
        print_unmatched_values(
            joined.filter(F.col("date_key").isNull()),
            "trade_date",
            "UNMATCHED_TRADE_DATES",
        )
    if missing_sources:
        print_unmatched_values(
            joined.filter(F.col("source_key").isNull()),
            "selected_source",
            "UNMATCHED_FUTURES_SOURCES",
        )

    if (
        joined_total != input_total
        or missing_dates != 0
        or missing_sources != 0
    ):
        raise RuntimeError(
            "Futures dimension join changed row count or produced missing keys"
        )

    # Silver grain is one row per trade date, so date_key is the stable fact key.
    fact = joined.select(
        F.col("date_key").cast("bigint").alias("futures_fact_key"),
        F.col("date_key").cast("int").alias("trade_date_key"),
        F.col("source_key").cast("bigint").alias("source_key"),
        F.col("open_price").cast("decimal(18,2)").alias("open_price"),
        F.col("high_price").cast("decimal(18,2)").alias("high_price"),
        F.col("low_price").cast("decimal(18,2)").alias("low_price"),
        F.col("last_price").cast("decimal(18,2)").alias("last_price"),
        F.col("volume_normalized").cast("decimal(20,2)").alias(
            "volume_normalized"
        ),
        F.col("change_percent").cast("decimal(12,3)").alias("change_percent"),
        F.col("ohlc_review"),
        F.col("volume_missing"),
        F.col("volume_invalid"),
        F.col("change_missing"),
        F.col("raw_version_conflict"),
        F.col("quality_status"),
        F.col("raw_observation_count").cast("bigint").alias(
            "raw_observation_count"
        ),
        F.col("distinct_raw_version_count").cast("bigint").alias(
            "distinct_raw_version_count"
        ),
        F.col("selected_bronze_record_id"),
        F.col("selected_bronze_object_key"),
        F.col("selected_bronze_record_position").cast("bigint").alias(
            "selected_bronze_record_position"
        ),
        F.current_timestamp().alias("gold_processed_at"),
    ).cache()

    try:
        total = fact.count()
        unique = fact.select("futures_fact_key").distinct().count()
        null_fact_keys = fact.filter(
            F.col("futures_fact_key").isNull()
        ).count()

        print("FACT_FUTURES_PRICE_ROWS:", total, flush=True)
        print("FACT_FUTURES_PRICE_UNIQUE_KEYS:", unique, flush=True)
        print("FACT_FUTURES_PRICE_NULL_KEYS:", null_fact_keys, flush=True)

        if (
            total != input_total
            or unique != total
            or null_fact_keys != 0
        ):
            raise RuntimeError(
                "Gold Futures candidate has missing or duplicate fact keys"
            )

        # Preserve the existing source-lineage checks.
        null_lineage = fact.filter(
            F.col("selected_bronze_record_id").isNull()
            | F.col("selected_bronze_object_key").isNull()
            | F.col("selected_bronze_record_position").isNull()
        ).count()

        print("FUTURES_NULL_SOURCE_LINEAGE:", null_lineage, flush=True)

        if null_lineage != 0:
            raise RuntimeError("Gold Futures source lineage is incomplete")

        print("=== FUTURES QUALITY DISTRIBUTION ===", flush=True)

        fact.groupBy("quality_status").count().orderBy(
            "quality_status"
        ).show(truncate=False)

        ohlc_review_count = fact.filter(
            F.col("ohlc_review") == True
        ).count()
        missing_volume_count = fact.filter(
            F.col("volume_missing") == True
        ).count()
        invalid_volume_count = fact.filter(
            F.col("volume_invalid") == True
        ).count()
        missing_change_count = fact.filter(
            F.col("change_missing") == True
        ).count()
        raw_conflict_count = fact.filter(
            F.col("raw_version_conflict") == True
        ).count()

        # These are observed metrics, not fixed acceptance thresholds.
        print("FUTURES_OHLC_REVIEW:", ohlc_review_count, flush=True)
        print("FUTURES_VOLUME_MISSING:", missing_volume_count, flush=True)
        print("FUTURES_VOLUME_INVALID:", invalid_volume_count, flush=True)
        print("FUTURES_CHANGE_MISSING:", missing_change_count, flush=True)
        print("FUTURES_RAW_VERSION_CONFLICT:", raw_conflict_count, flush=True)

        print("FACT_FUTURES_SAMPLE:", flush=True)
        fact.orderBy("trade_date_key").show(8, truncate=False)

        print("FACT_FUTURES_PRICE_VALIDATED", flush=True)

        if "--write" in sys.argv[1:]:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {TARGET} (
                    futures_fact_key BIGINT,
                    trade_date_key INT,
                    source_key BIGINT,
                    open_price DECIMAL(18,2),
                    high_price DECIMAL(18,2),
                    low_price DECIMAL(18,2),
                    last_price DECIMAL(18,2),
                    volume_normalized DECIMAL(20,2),
                    change_percent DECIMAL(12,3),
                    ohlc_review BOOLEAN,
                    volume_missing BOOLEAN,
                    volume_invalid BOOLEAN,
                    change_missing BOOLEAN,
                    raw_version_conflict BOOLEAN,
                    quality_status STRING,
                    raw_observation_count BIGINT,
                    distinct_raw_version_count BIGINT,
                    selected_bronze_record_id STRING,
                    selected_bronze_object_key STRING,
                    selected_bronze_record_position BIGINT,
                    gold_processed_at TIMESTAMP
                )
                USING iceberg
            """)

            existing = spark.table(TARGET)

            if set(existing.columns) != set(COLUMNS):
                raise RuntimeError(
                    "Gold Futures Fact schema mismatch: "
                    f"existing={existing.columns}, expected={COLUMNS}"
                )

            existing_total = existing.count()
            existing_unique = existing.select(
                "futures_fact_key"
            ).distinct().count()
            existing_null_keys = existing.filter(
                F.col("futures_fact_key").isNull()
            ).count()

            print("EXISTING_GOLD_FUTURES_ROWS:", existing_total, flush=True)
            print("EXISTING_GOLD_FUTURES_UNIQUE_KEYS:", existing_unique, flush=True)
            print("EXISTING_GOLD_FUTURES_NULL_KEYS:", existing_null_keys, flush=True)

            if (
                existing_total != existing_unique
                or existing_null_keys != 0
            ):
                raise RuntimeError(
                    "Existing Gold Futures Fact has duplicate or NULL keys"
                )

            # Refuse to change any previously stored business or lineage values.
            # gold_processed_at is excluded because it records processing time.
            comparison = fact.alias("incoming").join(
                existing.alias("stored"),
                on="futures_fact_key",
                how="inner",
            )

            mismatch = None
            for name in BUSINESS_COLUMNS:
                if name == "futures_fact_key":
                    continue

                difference = ~F.col(
                    f"incoming.{name}"
                ).eqNullSafe(F.col(f"stored.{name}"))

                mismatch = (
                    difference if mismatch is None
                    else mismatch | difference
                )

            mismatch_count = comparison.filter(mismatch).limit(1).count()
            if mismatch_count:
                raise RuntimeError(
                    "Existing Gold Futures row differs from current Silver; "
                    "refusing to overwrite historical data"
                )

            new_records = fact.select("futures_fact_key").join(
                existing.select("futures_fact_key"),
                on="futures_fact_key",
                how="left_anti",
            ).count()

            print("FACT_FUTURES_NEW_RECORDS:", new_records, flush=True)

            if new_records:
                fact.select(*COLUMNS).createOrReplaceTempView(
                    "stage_fact_futures_price"
                )

                # Append-only: existing rows are never updated or deleted.
                spark.sql(f"""
                    MERGE INTO {TARGET} AS t
                    USING stage_fact_futures_price AS s
                    ON t.futures_fact_key = s.futures_fact_key
                    WHEN NOT MATCHED THEN INSERT *
                """)

            saved = spark.table(TARGET)

            actual = saved.count()
            actual_unique = saved.select(
                "futures_fact_key"
            ).distinct().count()
            actual_null_keys = saved.filter(
                F.col("futures_fact_key").isNull()
            ).count()

            expected_total = existing_total + new_records

            missing_input_keys = fact.select(
                "futures_fact_key"
            ).join(
                saved.select("futures_fact_key"),
                on="futures_fact_key",
                how="left_anti",
            ).count()

            print("FACT_FUTURES_EXPECTED_TOTAL:", expected_total, flush=True)
            print("FACT_FUTURES_PRICE_ICEBERG_TOTAL:", actual, flush=True)
            print("FACT_FUTURES_PRICE_ICEBERG_UNIQUE_KEYS:", actual_unique, flush=True)
            print("FACT_FUTURES_PRICE_ICEBERG_NULL_KEYS:", actual_null_keys, flush=True)
            print("FACT_FUTURES_SILVER_KEYS_MISSING_FROM_GOLD:", missing_input_keys, flush=True)

            if (
                actual != expected_total
                or actual_unique != actual
                or actual_null_keys != 0
                or missing_input_keys != 0
            ):
                raise RuntimeError(
                    "Gold Futures append-only post-write validation failed"
                )

            print("FACT_FUTURES_PRICE_ICEBERG_PASSED", flush=True)

        else:
            print("FACT_FUTURES_PRICE_DRY_RUN_PASSED", flush=True)
            print("NO_GOLD_FACT_WRITTEN", flush=True)

    finally:
        fact.unpersist()

finally:
    spark.stop()
