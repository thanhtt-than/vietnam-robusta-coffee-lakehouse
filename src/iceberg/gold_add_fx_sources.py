"""Add FX source codes to the existing Gold source dimension."""

from pyspark.sql import functions as F
from minio_direct import spark_session

SOURCE = "lakehouse.silver.exchange_rate_observations"
TARGET = "lakehouse.gold.dim_source"

spark = spark_session("TLCN-Gold-Add-FX-Sources")

try:
    existing = spark.table(TARGET)

    expected_columns = {
        "source_key", "source_code", "source_name"
    }

    if set(existing.columns) != expected_columns:
        raise RuntimeError("Unexpected dim_source schema")

    fx = (
        spark.table(SOURCE)
        .select(F.col("source").alias("source_code"))
        .distinct()
    )

    if fx.count() != 5:
        raise RuntimeError("Expected exactly 5 FX sources")

    additions = fx.select(
        F.expr(
            "CAST(CONV(SUBSTR(SHA2(source_code, 256), "
            "1, 15), 16, 10) AS BIGINT)"
        ).alias("source_key"),
        "source_code",
        F.col("source_code").alias("source_name")
    )

    # Validate keys before modifying Gold.
    if additions.filter(
        "source_key IS NULL OR source_code IS NULL"
    ).limit(1).count():
        raise RuntimeError("Null source key or code")

    if additions.select("source_key").distinct().count() != 5:
        raise RuntimeError("Hash collision among FX sources")

    collisions = (
        additions.alias("a")
        .join(existing.alias("e"), "source_key", "inner")
        .filter(F.col("a.source_code") != F.col("e.source_code"))
    )

    if collisions.limit(1).count():
        raise RuntimeError("Source key collision with existing Gold")

    missing = additions.join(
        existing.select("source_code"),
        "source_code",
        "left_anti"
    )

    print("FX_SOURCES_TO_ADD:", missing.count(), flush=True)

    additions.createOrReplaceTempView("fx_source_stage")

    spark.sql(f"""
        MERGE INTO {TARGET} AS t
        USING fx_source_stage AS s
        ON t.source_key = s.source_key
        WHEN NOT MATCHED THEN INSERT (
            source_key, source_code, source_name
        ) VALUES (
            s.source_key, s.source_code, s.source_name
        )
    """)

    result = spark.table(TARGET)

    remaining = fx.join(
        result.select("source_code"),
        "source_code",
        "left_anti"
    ).count()

    print("FX_SOURCES_STILL_MISSING:", remaining, flush=True)
    print("DIM_SOURCE_TOTAL:", result.count(), flush=True)

    if remaining != 0:
        raise RuntimeError("FX source integration incomplete")

    if (
        result.count()
        != result.select("source_key").distinct().count()
    ):
        raise RuntimeError("Duplicate dim_source keys")

    print("GOLD_FX_SOURCES_PASSED", flush=True)

finally:
    spark.stop()
