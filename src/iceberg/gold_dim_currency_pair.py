"""Gold dimension for FX currency pairs."""

from pyspark.sql import functions as F
from minio_direct import spark_session

SOURCE = "lakehouse.silver.exchange_rate_observations"
TARGET = "lakehouse.gold.dim_currency_pair"

spark = spark_session("TLCN-Gold-Dim-Currency-Pair")

try:
    silver = spark.table(SOURCE)

    pairs = silver.select("currency_pair").distinct()

    expected = {"USD/VND", "EUR/VND"}
    actual = {
        row.currency_pair for row in pairs.collect()
    }

    if actual != expected:
        raise RuntimeError(
            f"Unexpected currency pairs: {actual}"
        )

    dimension = pairs.select(
        F.expr(
            "CAST(CONV(SUBSTR(SHA2(currency_pair, 256), "
            "1, 15), 16, 10) AS BIGINT)"
        ).alias("currency_pair_key"),
        "currency_pair",
        F.split("currency_pair", "/").getItem(0)
         .alias("base_currency"),
        F.split("currency_pair", "/").getItem(1)
         .alias("quote_currency"),
        F.lit("VND per 1 unit of base currency")
         .alias("rate_unit")
    )

    if (
        dimension.count() != 2
        or dimension.select("currency_pair_key").distinct().count() != 2
    ):
        raise RuntimeError("Invalid currency pair dimension")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET} (
            currency_pair_key BIGINT,
            currency_pair STRING,
            base_currency STRING,
            quote_currency STRING,
            rate_unit STRING
        ) USING iceberg
    """)

    existing = spark.table(TARGET)

    if set(existing.columns) != set(dimension.columns):
        raise RuntimeError("Currency dimension schema mismatch")

    dimension.createOrReplaceTempView("currency_pair_stage")

    spark.sql(f"""
        MERGE INTO {TARGET} AS t
        USING currency_pair_stage AS s
        ON t.currency_pair_key = s.currency_pair_key
        WHEN NOT MATCHED THEN INSERT *
    """)

    result = spark.table(TARGET)

    result.orderBy("currency_pair").show(truncate=False)

    total = result.count()
    unique = result.select("currency_pair_key").distinct().count()

    print("DIM_CURRENCY_PAIR_TOTAL:", total, flush=True)
    print("DIM_CURRENCY_PAIR_UNIQUE_KEYS:", unique, flush=True)

    if total != 2 or unique != 2:
        raise RuntimeError("Currency dimension validation failed")

    print("GOLD_DIM_CURRENCY_PAIR_PASSED", flush=True)

finally:
    spark.stop()
