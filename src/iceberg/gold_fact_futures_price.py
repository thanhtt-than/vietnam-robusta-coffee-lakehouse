import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.fact_futures_price"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-Futures-Price")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
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

spark.sparkContext.setLogLevel("ERROR")

try:
    london = spark.table(
        "lakehouse.silver.london_futures_daily"
    )

    dates = spark.table(
        "lakehouse.gold.dim_date"
    ).select(
        "date_key", "full_date"
    )

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select(
        "source_key", "source_code"
    )

    input_total = london.count()
    print("FUTURES_SILVER_TOTAL:", input_total, flush=True)

    assert input_total == 4755

    # Protect against dimension join multiplication.
    assert dates.count() == (
        dates.select("full_date").distinct().count()
    )

    assert sources.count() == (
        sources.select("source_code").distinct().count()
    )

    assert london.select("trade_date").distinct().count() == input_total

    # Join existing Date and Source dimensions.
    joined = (
        london.alias("f")
        .join(
            dates.alias("d"),
            F.col("f.trade_date") == F.col("d.full_date"),
            "left"
        )
        .join(
            sources.alias("s"),
            F.col("f.selected_source")
            == F.col("s.source_code"),
            "left"
        )
    )

    for key in ["date_key", "source_key"]:
        unmatched = joined.filter(
            F.col(key).isNull()
        ).count()

        print(
            "UNMATCHED_FK:", key, unmatched,
            flush=True
        )

        assert unmatched == 0

    # One futures observation per trading day.
    # A calendar date key is a stable BIGINT Fact key
    # because the Silver grain is exactly one trade date.
    fact = joined.select(
        F.col("date_key").cast("bigint").alias(
            "futures_fact_key"
        ),
        F.col("date_key").cast("int").alias(
            "trade_date_key"
        ),
        F.col("source_key").cast("bigint"),

        F.col("open_price").cast("decimal(18,2)"),
        F.col("high_price").cast("decimal(18,2)"),
        F.col("low_price").cast("decimal(18,2)"),
        F.col("last_price").cast("decimal(18,2)"),

        F.col("volume_normalized").cast("decimal(20,2)"),
        F.col("change_percent").cast("decimal(12,3)"),

        F.col("ohlc_review"),
        F.col("volume_missing"),
        F.col("volume_invalid"),
        F.col("change_missing"),
        F.col("raw_version_conflict"),

        F.col("quality_status"),
        F.col("raw_observation_count"),
        F.col("distinct_raw_version_count"),

        F.col("selected_bronze_record_id"),
        F.col("selected_bronze_object_key"),
        F.col("selected_bronze_record_position"),

        F.current_timestamp().alias("gold_processed_at")
    ).cache()

    total = fact.count()
    unique = fact.select(
        "futures_fact_key"
    ).distinct().count()

    print("FACT_FUTURES_PRICE_ROWS:", total, flush=True)
    print("FACT_FUTURES_PRICE_UNIQUE_KEYS:", unique, flush=True)

    assert total == input_total == unique

    # All source lineage must remain available.
    null_lineage = fact.filter(
        F.col("selected_bronze_record_id").isNull()
    ).count()

    assert null_lineage == 0

    print("FUTURES_QUALITY_STATUS:", flush=True)

    fact.groupBy("quality_status").count().show(
        truncate=False
    )

    ohlc_count = fact.filter(
        F.col("ohlc_review") == True
    ).count()

    missing_volume_count = fact.filter(
        F.col("volume_missing") == True
    ).count()

    print("FUTURES_OHLC_REVIEW:", ohlc_count, flush=True)
    print(
        "FUTURES_VOLUME_MISSING:",
        missing_volume_count,
        flush=True
    )

    assert ohlc_count == 374
    assert missing_volume_count == 345

    print("FACT_FUTURES_SAMPLE:", flush=True)

    fact.orderBy("trade_date_key").show(
        8, truncate=False
    )

    print("FACT_FUTURES_PRICE_VALIDATED", flush=True)

    if WRITE:
        spark.sql("""
            CREATE NAMESPACE IF NOT EXISTS lakehouse.gold
        """)

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

        if spark.table(TARGET).columns != fact.columns:
            raise RuntimeError(
                "Fact Futures schema mismatch"
            )

        fact.createOrReplaceTempView(
            "stage_fact_futures_price"
        )

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_futures_price s
            ON t.futures_fact_key = s.futures_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        saved = spark.table(TARGET)

        actual = saved.count()
        actual_unique = saved.select(
            "futures_fact_key"
        ).distinct().count()

        print(
            "FACT_FUTURES_PRICE_ICEBERG_TOTAL:",
            actual,
            flush=True
        )

        assert actual == total == actual_unique

        print(
            "FACT_FUTURES_PRICE_ICEBERG_PASSED",
            flush=True
        )

    else:
        print(
            "FACT_FUTURES_PRICE_DRY_RUN_PASSED",
            flush=True
        )
        print("NO_GOLD_FACT_WRITTEN", flush=True)

finally:
    spark.stop()
