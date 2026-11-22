"""Safely extend existing Gold dimensions for monthly coffee trade."""

import sys

from pyspark.sql import functions as F
from minio_direct import spark_session

WRITE = "--write" in sys.argv[1:]

DESTINATION = "lakehouse.gold.dim_destination"
SOURCE = "lakehouse.gold.dim_source"


def hash_key(column):
    return F.expr(
        "CAST(CONV(SUBSTR(SHA2("
        + column +
        ", 256), 1, 15), 16, 10) AS BIGINT)"
    )


def prepare_additions(spark):
    destinations = spark.createDataFrame(
        [
            ("EU", "European Union", False),
            ("ASEAN", "Association of Southeast Asian Nations", False),
        ],
        ["partner_code", "partner_name", "is_world_aggregate"],
    ).withColumn(
        "destination_key", hash_key("partner_code")
    ).select(
        "destination_key",
        "partner_code",
        "partner_name",
        "is_world_aggregate",
    )

    sources = spark.createDataFrame(
        [("nguoi2_customs_csv", "Nguoi 2 customs export CSV")],
        ["source_code", "source_name"],
    ).withColumn(
        "source_key", hash_key("source_code")
    ).select(
        "source_key", "source_code", "source_name"
    )

    return destinations, sources


def inspect(existing, additions, key, natural, label):
    if set(existing.columns) != set(additions.columns):
        raise RuntimeError(label + ": schema mismatch")

    if existing.count() != existing.select(key).distinct().count():
        raise RuntimeError(label + ": duplicate existing primary keys")

    if existing.count() != existing.select(natural).distinct().count():
        raise RuntimeError(label + ": duplicate existing natural keys")

    if additions.count() != additions.select(key).distinct().count():
        raise RuntimeError(label + ": duplicate addition keys")

    collision = (
        additions.alias("a")
        .join(existing.alias("e"), key, "inner")
        .filter(F.col("a." + natural) != F.col("e." + natural))
    )

    if collision.limit(1).count():
        raise RuntimeError(label + ": primary key collision")

    common = additions.alias("a").join(
        existing.alias("e"), natural, "inner"
    )

    mismatch = None
    for column in additions.columns:
        term = ~F.col("a." + column).eqNullSafe(
            F.col("e." + column)
        )
        mismatch = term if mismatch is None else mismatch | term

    if common.filter(mismatch).limit(1).count():
        raise RuntimeError(label + ": existing record differs")

    missing = additions.join(
        existing.select(natural),
        natural,
        "left_anti"
    )

    print(label + "_NEW_RECORDS:", missing.count(), flush=True)

    return missing


def merge_additions(spark, target, additions, natural, stage):
    additions.createOrReplaceTempView(stage)

    columns = ", ".join(additions.columns)
    values = ", ".join("s." + c for c in additions.columns)

    spark.sql(f"""
        MERGE INTO {target} t
        USING {stage} s
        ON t.{natural} = s.{natural}
        WHEN NOT MATCHED THEN
          INSERT ({columns}) VALUES ({values})
    """)


def main():
    spark = spark_session("TLCN-Gold-Add-Trade-Dimensions")

    try:
        existing_destinations = spark.table(DESTINATION)
        existing_sources = spark.table(SOURCE)

        destinations, sources = prepare_additions(spark)

        print(
            "DESTINATION_BEFORE:",
            existing_destinations.count(),
            flush=True,
        )
        print(
            "SOURCE_BEFORE:",
            existing_sources.count(),
            flush=True,
        )

        destination_missing = inspect(
            existing_destinations,
            destinations,
            "destination_key",
            "partner_code",
            "TRADE_DESTINATION",
        )

        source_missing = inspect(
            existing_sources,
            sources,
            "source_key",
            "source_code",
            "TRADE_SOURCE",
        )

        if not WRITE:
            print("TRADE_DIMENSIONS_DRY_RUN_PASSED", flush=True)
            return

        if destination_missing.count():
            merge_additions(
                spark, DESTINATION, destination_missing,
                "partner_code", "trade_destination_stage"
            )

        if source_missing.count():
            merge_additions(
                spark, SOURCE, source_missing,
                "source_code", "trade_source_stage"
            )

        final_destinations = spark.table(DESTINATION)
        final_sources = spark.table(SOURCE)

        for label, df, additions, natural, key in [
            (
                "TRADE_DESTINATION",
                final_destinations,
                destinations,
                "partner_code",
                "destination_key",
            ),
            (
                "TRADE_SOURCE",
                final_sources,
                sources,
                "source_code",
                "source_key",
            ),
        ]:
            if additions.join(
                df.select(natural),
                natural,
                "left_anti"
            ).count():
                raise RuntimeError(label + ": missing new values")

            if df.count() != df.select(key).distinct().count():
                raise RuntimeError(label + ": duplicate primary keys")

            if df.count() != df.select(natural).distinct().count():
                raise RuntimeError(label + ": duplicate natural keys")

        print(
            "DIM_DESTINATION_TOTAL:",
            final_destinations.count(),
            flush=True,
        )
        print(
            "DIM_SOURCE_TOTAL:",
            final_sources.count(),
            flush=True,
        )

        print("GOLD_TRADE_DIMENSIONS_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
