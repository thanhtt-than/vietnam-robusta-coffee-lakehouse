"""Gold FX fact: append-only dimension joins and quality validation.

Default mode is a read-only dry run. Pass --write to append new observation IDs.
Existing Gold rows are never updated or deleted. If an incoming observation ID
already exists with different values, the job stops rather than overwriting it.
"""

import sys

from pyspark.sql import functions as F
from minio_direct import spark_session


SOURCE = "lakehouse.silver.exchange_rate_observations"
TARGET = "lakehouse.gold.fact_exchange_rate"

COLUMNS = [
    "observation_id",
    "date_key",
    "source_key",
    "currency_pair_key",
    "observation_date",
    "currency_pair",
    "source",
    "frequency",
    "rate_type",
    "rate_vnd",
    "bronze_record_id",
    "object_key",
]


def require_columns(df, required, label):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise RuntimeError(f"{label} missing columns: {missing}")


def validate_dimension(df, name, natural_key, surrogate_key):
    total = df.count()
    null_natural = df.filter(F.col(natural_key).isNull()).count()
    null_surrogate = df.filter(F.col(surrogate_key).isNull()).count()
    unique_natural = df.select(natural_key).distinct().count()

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


spark = spark_session("TLCN-Gold-Fact-Exchange-Rate")

try:
    silver = spark.table(SOURCE)

    require_columns(
        silver,
        [
            "observation_id",
            "observation_date",
            "currency_pair",
            "source",
            "frequency",
            "rate_type",
            "rate_vnd",
            "bronze_record_id",
            "object_key",
        ],
        "Silver FX",
    )

    dates = spark.table("lakehouse.gold.dim_date").select(
        "date_key", "full_date"
    )
    sources = spark.table("lakehouse.gold.dim_source").select(
        "source_key", "source_code"
    )
    pairs = spark.table("lakehouse.gold.dim_currency_pair").select(
        "currency_pair_key", "currency_pair"
    )

    print("=== INPUT VALIDATION ===", flush=True)

    input_count = silver.count()
    unique_count = silver.select("observation_id").distinct().count()
    null_ids = silver.filter(F.col("observation_id").isNull()).count()
    empty_ids = silver.filter(
        F.trim(F.col("observation_id")) == ""
    ).count()

    print("SILVER_FX_ROWS:", input_count, flush=True)
    print("SILVER_UNIQUE_IDS:", unique_count, flush=True)
    print("SILVER_NULL_IDS:", null_ids, flush=True)
    print("SILVER_EMPTY_IDS:", empty_ids, flush=True)

    # Silver is appendable: never pin validation to a historical row count.
    if (
        input_count <= 0
        or unique_count != input_count
        or null_ids != 0
        or empty_ids != 0
    ):
        raise RuntimeError("Invalid Silver FX rows or observation IDs")

    validate_dimension(dates, "dim_date", "full_date", "date_key")
    validate_dimension(sources, "dim_source", "source_code", "source_key")
    validate_dimension(
        pairs, "dim_currency_pair", "currency_pair", "currency_pair_key"
    )

    print("=== JOIN DIMENSIONS ===", flush=True)

    fact = (
        silver.alias("f")
        .join(
            dates.alias("d"),
            F.col("f.observation_date") == F.col("d.full_date"),
            "left",
        )
        .join(
            sources.alias("s"),
            F.col("f.source") == F.col("s.source_code"),
            "left",
        )
        .join(
            pairs.alias("p"),
            F.col("f.currency_pair") == F.col("p.currency_pair"),
            "left",
        )
        .select(
            F.col("f.observation_id"),
            F.col("d.date_key"),
            F.col("s.source_key"),
            F.col("p.currency_pair_key"),
            F.col("f.observation_date"),
            F.col("f.currency_pair"),
            F.col("f.source"),
            F.col("f.frequency"),
            F.col("f.rate_type"),
            F.col("f.rate_vnd"),
            F.col("f.bronze_record_id"),
            F.col("f.object_key"),
        )
    ).cache()

    try:
        fact_count = fact.count()

        print("FACT_CANDIDATE_ROWS:", fact_count, flush=True)

        missing_dates = fact.filter(F.col("date_key").isNull()).count()
        missing_sources = fact.filter(F.col("source_key").isNull()).count()
        missing_pairs = fact.filter(
            F.col("currency_pair_key").isNull()
        ).count()

        print("MISSING_DATE_KEYS:", missing_dates, flush=True)
        print("MISSING_SOURCE_KEYS:", missing_sources, flush=True)
        print("MISSING_CURRENCY_PAIR_KEYS:", missing_pairs, flush=True)

        if missing_dates:
            print_unmatched_values(
                fact.filter(F.col("date_key").isNull()),
                "observation_date",
                "UNMATCHED_DATES",
            )
        if missing_sources:
            print_unmatched_values(
                fact.filter(F.col("source_key").isNull()),
                "source",
                "UNMATCHED_SOURCES",
            )
        if missing_pairs:
            print_unmatched_values(
                fact.filter(F.col("currency_pair_key").isNull()),
                "currency_pair",
                "UNMATCHED_CURRENCY_PAIRS",
            )

        unique_fact_ids = fact.select("observation_id").distinct().count()
        null_fact_ids = fact.filter(
            F.col("observation_id").isNull()
        ).count()
        invalid_rates = fact.filter(
            F.col("rate_vnd").isNull() | (F.col("rate_vnd") <= 0)
        ).count()

        print("FACT_UNIQUE_IDS:", unique_fact_ids, flush=True)
        print("FACT_NULL_IDS:", null_fact_ids, flush=True)
        print("INVALID_RATES:", invalid_rates, flush=True)

        # A dimension join must preserve exactly one fact row per Silver row.
        if (
            fact_count != input_count
            or unique_fact_ids != fact_count
            or null_fact_ids != 0
            or missing_dates != 0
            or missing_sources != 0
            or missing_pairs != 0
            or invalid_rates != 0
        ):
            raise RuntimeError("Gold FX fact validation failed")

        print("=== FACT DISTRIBUTION ===", flush=True)
        fact.groupBy(
            "currency_pair", "frequency", "rate_type"
        ).count().orderBy(
            "currency_pair", "frequency", "rate_type"
        ).show(truncate=False)

        if "--write" in sys.argv[1:]:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {TARGET} (
                    observation_id STRING,
                    date_key INT,
                    source_key BIGINT,
                    currency_pair_key BIGINT,
                    observation_date DATE,
                    currency_pair STRING,
                    source STRING,
                    frequency STRING,
                    rate_type STRING,
                    rate_vnd DECIMAL(28,12),
                    bronze_record_id STRING,
                    object_key STRING
                ) USING iceberg
            """)

            existing = spark.table(TARGET)

            if set(existing.columns) != set(COLUMNS):
                raise RuntimeError("Gold Fact schema mismatch")

            existing_count = existing.count()
            existing_unique = existing.select(
                "observation_id"
            ).distinct().count()
            existing_null_ids = existing.filter(
                F.col("observation_id").isNull()
            ).count()

            print("EXISTING_GOLD_ROWS:", existing_count, flush=True)
            print("EXISTING_GOLD_UNIQUE_IDS:", existing_unique, flush=True)
            print("EXISTING_GOLD_NULL_IDS:", existing_null_ids, flush=True)

            if (
                existing_count != existing_unique
                or existing_null_ids != 0
            ):
                raise RuntimeError(
                    "Existing Gold Fact contains duplicate or NULL IDs"
                )

            # Preserve old values: overlapping IDs must be identical.
            comparison = fact.alias("incoming").join(
                existing.alias("stored"),
                on="observation_id",
                how="inner",
            )

            mismatch = None
            for name in COLUMNS:
                if name == "observation_id":
                    continue

                difference = ~F.col(
                    f"incoming.{name}"
                ).eqNullSafe(F.col(f"stored.{name}"))

                mismatch = (
                    difference if mismatch is None
                    else mismatch | difference
                )

            if comparison.filter(mismatch).limit(1).count():
                raise RuntimeError(
                    "Existing Gold Fact differs from Silver; refusing overwrite"
                )

            new_records = fact.select("observation_id").join(
                existing.select("observation_id"),
                on="observation_id",
                how="left_anti",
            ).count()

            print("FACT_NEW_RECORDS:", new_records, flush=True)

            if new_records:
                fact.select(*COLUMNS).createOrReplaceTempView(
                    "fx_fact_stage"
                )

                spark.sql(f"""
                    MERGE INTO {TARGET} AS t
                    USING fx_fact_stage AS s
                    ON t.observation_id = s.observation_id
                    WHEN NOT MATCHED THEN INSERT *
                """)

            result = spark.table(TARGET)
            final_count = result.count()
            final_unique = result.select(
                "observation_id"
            ).distinct().count()
            final_null_ids = result.filter(
                F.col("observation_id").isNull()
            ).count()

            expected_final_count = existing_count + new_records

            # Verify append-only behavior and that every current Silver ID
            # exists in Gold after the MERGE.
            missing_input_ids = fact.select("observation_id").join(
                result.select("observation_id"),
                on="observation_id",
                how="left_anti",
            ).count()

            print("FACT_EXPECTED_FINAL_ROWS:", expected_final_count, flush=True)
            print("FACT_EXCHANGE_RATE_TOTAL:", final_count, flush=True)
            print("FACT_EXCHANGE_RATE_UNIQUE_IDS:", final_unique, flush=True)
            print("FACT_EXCHANGE_RATE_NULL_IDS:", final_null_ids, flush=True)
            print("FACT_SILVER_IDS_MISSING_FROM_GOLD:", missing_input_ids, flush=True)

            if (
                final_count != expected_final_count
                or final_unique != final_count
                or final_null_ids != 0
                or missing_input_ids != 0
            ):
                raise RuntimeError(
                    "Gold Fact append-only post-write validation failed"
                )

            print("GOLD_FACT_EXCHANGE_RATE_ICEBERG_PASSED", flush=True)

        else:
            print("GOLD_FACT_EXCHANGE_RATE_DRY_RUN_PASSED", flush=True)
            print("NO_GOLD_TABLE_WRITTEN", flush=True)

    finally:
        fact.unpersist()

finally:
    spark.stop()
