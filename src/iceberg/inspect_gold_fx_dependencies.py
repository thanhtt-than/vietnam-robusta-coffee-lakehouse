from minio_direct import spark_session
from pyspark.sql import functions as F

spark = spark_session("TLCN-Inspect-Gold-FX-Dependencies")

try:
    fx = spark.table(
        "lakehouse.silver.exchange_rate_observations"
    )

    dates = spark.table(
        "lakehouse.gold.dim_date"
    ).select("date_key", "full_date")

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select("source_key", "source_code")

    print("=== SILVER FX ===", flush=True)
    print("FX_OBSERVATIONS:", fx.count(), flush=True)

    fx.select(
        F.min("observation_date").alias("min_date"),
        F.max("observation_date").alias("max_date")
    ).show(truncate=False)

    print("=== DATE DIMENSION ===", flush=True)

    missing_dates = (
        fx.select("observation_date").distinct()
        .join(
            dates,
            F.col("observation_date") == F.col("full_date"),
            "left_anti"
        )
    )

    print(
        "MISSING_DATE_KEYS:",
        missing_dates.count(),
        flush=True
    )

    print("=== SOURCE DIMENSION ===", flush=True)

    fx_sources = fx.select(
        F.col("source").alias("source_code")
    ).distinct()

    missing_sources = fx_sources.join(
        sources,
        on="source_code",
        how="left_anti"
    )

    print(
        "MISSING_SOURCE_CODES:",
        missing_sources.count(),
        flush=True
    )

    missing_sources.show(truncate=False)

    print("GOLD_FX_DEPENDENCY_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
