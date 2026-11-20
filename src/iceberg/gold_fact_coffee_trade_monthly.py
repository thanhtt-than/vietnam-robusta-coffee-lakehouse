"""Build monthly coffee export Gold fact from Silver Iceberg."""

import sys

from pyspark.sql import functions as F
from minio_direct import spark_session

WRITE = "--write" in sys.argv[1:]

SILVER = "lakehouse.silver.coffee_trade_monthly"
TARGET = "lakehouse.gold.fact_coffee_trade_monthly"

MARKET_MAPPING = [
    ("ALL_MARKETS", "000"),
    ("AN_DO", "699"),
    ("ASEAN", "ASEAN"),
    ("CHND_TRUNG_HOA", "156"),
    ("EU", "EU"),
    ("HAN_QUOC", "410"),
    ("MY", "842"),
    ("NHAT_BAN", "392"),
    ("O_XTRAY_LI_A", "036"),
    ("U_CRAI_NA", "804"),
]

COLUMNS = [
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

DDL = """
    trade_fact_key STRING,
    date_key INT,
    destination_key BIGINT,
    source_key BIGINT,
    geography_key INT,
    year INT,
    month INT,
    market_code STRING,
    market_type STRING,
    quantity_tonnes DECIMAL(20,3),
    value_usd_thousand DECIMAL(20,3),
    quality_status STRING,
    bronze_record_id STRING,
    object_key STRING,
    record_position BIGINT,
    silver_observation_id STRING
"""


def require_unique(df, column, label):
    total = df.count()
    unique = df.select(column).distinct().count()

    print(label + "_ROWS:", total, flush=True)
    print(label + "_UNIQUE:", unique, flush=True)

    if total != unique:
        raise RuntimeError("Duplicate dimension key: " + label)


def main():
    spark = spark_session("TLCN-Gold-Fact-Coffee-Trade-Monthly")

    try:
        silver = spark.table(SILVER)

        dates = spark.table(
            "lakehouse.gold.dim_date"
        ).filter(
            F.dayofmonth("full_date") == 1
        ).select(
            "date_key", "year", "month"
        )

        destinations = spark.table(
            "lakehouse.gold.dim_destination"
        ).select(
            "destination_key", "partner_code"
        )

        sources = spark.table(
            "lakehouse.gold.dim_source"
        ).filter(
            F.col("source_code") == "nguoi2_customs_csv"
        ).select("source_key")

        geography = spark.table(
            "lakehouse.gold.dim_geography"
        ).filter(
            F.col("geography_code") == "VN"
        ).select("geography_key")

        mapping = spark.createDataFrame(
            MARKET_MAPPING,
            ["market_code", "partner_code"]
        )

        print("=== TRADE GOLD FACT VALIDATION ===", flush=True)

        require_unique(mapping, "market_code", "MARKET_MAPPING")
        require_unique(destinations, "partner_code", "DIM_DESTINATION")

        input_count = silver.count()
        print("SILVER_INPUT_ROWS:", input_count, flush=True)

        if input_count != 1312:
            raise RuntimeError("Unexpected Silver row count")

        require_unique(silver, "observation_id", "SILVER_OBSERVATIONS")

        national_count = silver.filter(
            F.col("market_code") == "ALL_MARKETS"
        ).count()

        market_count = silver.filter(
            F.col("market_code") != "ALL_MARKETS"
        ).count()

        print("NATIONAL_ROWS:", national_count, flush=True)
        print("MARKET_ROWS:", market_count, flush=True)

        if national_count != 141 or market_count != 1171:
            raise RuntimeError("Unexpected national/market split")

        if sources.count() != 1 or geography.count() != 1:
            raise RuntimeError("Source or geography not unique")

        joined = (
            silver.alias("s")
            .join(
                mapping.alias("m"),
                F.col("s.market_code") == F.col("m.market_code"),
                "left"
            )
            .join(
                destinations.alias("d"),
                F.col("m.partner_code") == F.col("d.partner_code"),
                "left"
            )
            .join(
                dates.alias("dt"),
                (F.col("s.year") == F.col("dt.year")) &
                (F.col("s.month") == F.col("dt.month")),
                "left"
            )
            .crossJoin(sources.alias("src"))
            .crossJoin(geography.alias("geo"))
        ).cache()

        try:
            joined_count = joined.count()
            print("JOINED_ROWS:", joined_count, flush=True)

            if joined_count != input_count:
                raise RuntimeError("Fact JOIN changed row count")

            for label, column in [
                ("MARKET", "m.partner_code"),
                ("DESTINATION", "d.destination_key"),
                ("DATE", "dt.date_key"),
                ("SOURCE", "src.source_key"),
                ("GEOGRAPHY", "geo.geography_key"),
            ]:
                missing = joined.filter(
                    F.col(column).isNull()
                ).count()

                print("MISSING_" + label + ":", missing, flush=True)

                if missing:
                    raise RuntimeError("Missing FK: " + label)

            business_id = F.concat_ws(
                "||",
                F.lit("COFFEE_TRADE_MONTHLY"),
                F.col("s.year").cast("string"),
                F.col("s.month").cast("string"),
                F.col("s.market_code")
            )

            fact = joined.select(
                F.sha2(business_id, 256).alias("trade_fact_key"),

                F.col("dt.date_key").cast("int").alias("date_key"),
                F.col("d.destination_key").cast("bigint").alias(
                    "destination_key"
                ),
                F.col("src.source_key").cast("bigint").alias(
                    "source_key"
                ),
                F.col("geo.geography_key").cast("int").alias(
                    "geography_key"
                ),

                F.col("s.year").cast("int").alias("year"),
                F.col("s.month").cast("int").alias("month"),
                F.col("s.market_code").alias("market_code"),
                F.col("s.market_type").alias("market_type"),

                F.col("s.quantity_tonnes").cast(
                    "decimal(20,3)"
                ).alias("quantity_tonnes"),

                F.col("s.value_usd_thousand").cast(
                    "decimal(20,3)"
                ).alias("value_usd_thousand"),

                F.col("s.quality_status").alias("quality_status"),
                F.col("s.bronze_record_id").alias("bronze_record_id"),
                F.col("s.object_key").alias("object_key"),
                F.col("s.record_position").cast("bigint").alias(
                    "record_position"
                ),
                F.col("s.observation_id").alias("silver_observation_id")
            ).cache()

            try:
                total = fact.count()
                unique = fact.select("trade_fact_key").distinct().count()

                print("FACT_CANDIDATE_ROWS:", total, flush=True)
                print("FACT_UNIQUE_KEYS:", unique, flush=True)

                if total != unique or total != 1312:
                    raise RuntimeError("Invalid Fact candidate count")

                required = [
                    c for c in COLUMNS
                    if c not in (
                        "quantity_tonnes",
                        "value_usd_thousand"
                    )
                ]

                for column in required:
                    if fact.filter(
                        F.col(column).isNull()
                    ).limit(1).count():
                        raise RuntimeError(
                            "Null required column: " + column
                        )

                if fact.filter(
                    F.col("quantity_tonnes").isNull() &
                    F.col("value_usd_thousand").isNull()
                ).limit(1).count():
                    raise RuntimeError("Fact contains empty measurements")

                if not WRITE:
                    print("GOLD_TRADE_FACT_DRY_RUN_PASSED", flush=True)
                    return

                spark.sql(
                    "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
                )

                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {TARGET} "
                    f"({DDL}) USING iceberg"
                )

                stored = spark.table(TARGET)

                if set(stored.columns) != set(COLUMNS):
                    raise RuntimeError("Existing Fact schema mismatch")

                comparison = (
                    fact.alias("incoming")
                    .join(
                        stored.alias("stored"),
                        on="trade_fact_key",
                        how="inner"
                    )
                )

                mismatch = None

                for column in COLUMNS:
                    if column == "trade_fact_key":
                        continue

                    term = ~F.col(
                        "incoming." + column
                    ).eqNullSafe(
                        F.col("stored." + column)
                    )

                    mismatch = (
                        term if mismatch is None
                        else mismatch | term
                    )

                if comparison.filter(mismatch).limit(1).count():
                    raise RuntimeError(
                        "Existing Fact data differs from Silver"
                    )

                new_count = (
                    fact.select("trade_fact_key")
                    .join(
                        stored.select("trade_fact_key"),
                        on="trade_fact_key",
                        how="left_anti"
                    )
                    .count()
                )

                print("FACT_NEW_RECORDS:", new_count, flush=True)

                if new_count:
                    fact.createOrReplaceTempView(
                        "trade_fact_monthly_stage"
                    )

                    columns = ", ".join(COLUMNS)
                    values = ", ".join("s." + c for c in COLUMNS)

                    spark.sql(f"""
                        MERGE INTO {TARGET} t
                        USING trade_fact_monthly_stage s
                        ON t.trade_fact_key = s.trade_fact_key
                        WHEN NOT MATCHED THEN
                          INSERT ({columns}) VALUES ({values})
                    """)

                saved = spark.table(TARGET)
                saved_count = saved.count()

                print("GOLD_TRADE_FACT_TOTAL:", saved_count, flush=True)

                if saved_count != 1312:
                    raise RuntimeError("Unexpected Gold Fact total")

                if saved.select(
                    "trade_fact_key"
                ).distinct().count() != saved_count:
                    raise RuntimeError("Duplicate Gold Fact keys")

                print("GOLD_FACT_COFFEE_TRADE_MONTHLY_PASSED", flush=True)

            finally:
                fact.unpersist()

        finally:
            joined.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
