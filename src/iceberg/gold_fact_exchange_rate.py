"""Gold FX fact: dry-run dimension joins and quality validation."""

from pyspark.sql import functions as F
from minio_direct import spark_session

SOURCE = "lakehouse.silver.exchange_rate_observations"
TARGET = "lakehouse.gold.fact_exchange_rate"

spark = spark_session("TLCN-Gold-Fact-Exchange-Rate")

try:
    silver = spark.table(SOURCE)

    dates = spark.table(
        "lakehouse.gold.dim_date"
    ).select("date_key", "full_date")

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select("source_key", "source_code")

    pairs = spark.table(
        "lakehouse.gold.dim_currency_pair"
    ).select("currency_pair_key", "currency_pair")

    print("=== INPUT VALIDATION ===", flush=True)

    input_count = silver.count()
    unique_count = silver.select("observation_id").distinct().count()

    print("SILVER_FX_ROWS:", input_count, flush=True)
    print("SILVER_UNIQUE_IDS:", unique_count, flush=True)

    if input_count != 8166 or unique_count != input_count:
        raise RuntimeError("Invalid Silver FX record count or IDs")

    for name, df, key in [
        ("dim_date", dates, "full_date"),
        ("dim_source", sources, "source_code"),
        ("dim_currency_pair", pairs, "currency_pair"),
    ]:
        total = df.count()
        unique = df.select(key).distinct().count()

        print(f"{name}_ROWS:", total, flush=True)
        print(f"{name}_UNIQUE_KEYS:", unique, flush=True)

        if total != unique:
            raise RuntimeError(f"Duplicate dimension keys: {name}")

    print("=== JOIN DIMENSIONS ===", flush=True)

    fact = (
        silver.alias("f")
        .join(
            dates.alias("d"),
            F.col("f.observation_date") == F.col("d.full_date"),
            "left"
        )
        .join(
            sources.alias("s"),
            F.col("f.source") == F.col("s.source_code"),
            "left"
        )
        .join(
            pairs.alias("p"),
            F.col("f.currency_pair") == F.col("p.currency_pair"),
            "left"
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
            F.col("f.object_key")
        )
    ).cache()

    try:
        fact_count = fact.count()

        print("FACT_CANDIDATE_ROWS:", fact_count, flush=True)

        missing_dates = fact.filter(
            F.col("date_key").isNull()
        ).count()

        missing_sources = fact.filter(
            F.col("source_key").isNull()
        ).count()

        missing_pairs = fact.filter(
            F.col("currency_pair_key").isNull()
        ).count()

        print("MISSING_DATE_KEYS:", missing_dates, flush=True)
        print("MISSING_SOURCE_KEYS:", missing_sources, flush=True)
        print("MISSING_CURRENCY_PAIR_KEYS:", missing_pairs, flush=True)

        unique_fact_ids = fact.select(
            "observation_id"
        ).distinct().count()

        print("FACT_UNIQUE_IDS:", unique_fact_ids, flush=True)

        invalid_rates = fact.filter(
            F.col("rate_vnd").isNull()
            | (F.col("rate_vnd") <= 0)
        ).count()

        print("INVALID_RATES:", invalid_rates, flush=True)

        if (
            fact_count != input_count
            or unique_fact_ids != fact_count
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

        import sys

        if "--write" in sys.argv[1:]:
            columns = [
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

            if set(existing.columns) != set(columns):
                raise RuntimeError("Gold Fact schema mismatch")

            if existing.count() != existing.select(
                "observation_id"
            ).distinct().count():
                raise RuntimeError("Existing Gold Fact contains duplicate IDs")

            # Prevent silent changes to previously stored observations.
            comparison = fact.alias("incoming").join(
                existing.alias("stored"),
                on="observation_id",
                how="inner"
            )

            mismatch = None

            for name in columns:
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
                how="left_anti"
            ).count()

            print("FACT_NEW_RECORDS:", new_records, flush=True)

            if new_records:
                fact.select(*columns).createOrReplaceTempView(
                    "fx_fact_stage"
                )

                spark.sql(f"""
                    MERGE INTO {TARGET} AS t
                    USING fx_fact_stage AS s
                    ON t.observation_id = s.observation_id
                    WHEN NOT MATCHED THEN INSERT *
                """)

            result = spark.table(TARGET)

            total = result.count()
            unique = result.select(
                "observation_id"
            ).distinct().count()

            print("FACT_EXCHANGE_RATE_TOTAL:", total, flush=True)
            print("FACT_EXCHANGE_RATE_UNIQUE_IDS:", unique, flush=True)

            if total != 8166 or unique != total:
                raise RuntimeError("Gold Fact validation failed")

            print("GOLD_FACT_EXCHANGE_RATE_ICEBERG_PASSED", flush=True)

        else:
            print("GOLD_FACT_EXCHANGE_RATE_DRY_RUN_PASSED", flush=True)
            print("NO_GOLD_TABLE_WRITTEN", flush=True)

    finally:
        fact.unpersist()

finally:
    spark.stop()
