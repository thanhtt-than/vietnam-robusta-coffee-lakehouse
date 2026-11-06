import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.fact_fuel_price"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-Fuel-Price")
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


def verify_unique(df, keys, name):
    total = df.count()
    unique = df.select(*keys).distinct().count()

    print(
        "KEY_CHECK:", name,
        "ROWS:", total,
        "UNIQUE:", unique,
        flush=True
    )

    assert total == unique, (
        f"Duplicate business keys in {name}"
    )


def stable_hash(column_name):
    return F.expr(
        "CAST(CONV(SUBSTR(SHA2("
        + column_name +
        ", 256), 1, 15), 16, 10) AS BIGINT)"
    )


try:
    # 1. Load Silver Fuel.
    fuel = spark.table(
        "lakehouse.silver.fuel_price_observations"
    )

    dates = spark.table(
        "lakehouse.gold.dim_date"
    ).select("date_key", "full_date")

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select("source_key", "source_code")

    series = spark.table(
        "lakehouse.gold.dim_fuel_series"
    ).select("fuel_series_key", "fuel_series_id")

    input_total = fuel.count()

    print("FUEL_SILVER_TOTAL:", input_total, flush=True)

    assert input_total == 1704

    # 2. Check the natural grain.
    grain = [
        "source_family",
        "series_code",
        "region",
        "quote_date",
        "unit"
    ]

    verify_unique(
        fuel, grain, "silver.fuel_price_observations"
    )
    verify_unique(
        dates, ["full_date"], "gold.dim_date"
    )
    verify_unique(
        sources, ["source_code"], "gold.dim_source"
    )
    verify_unique(
        series, ["fuel_series_id"], "gold.dim_fuel_series"
    )

    # Every component is required to construct a
    # lossless composite business key.
    for column in grain:
        nulls = fuel.filter(
            F.col(column).isNull()
        ).count()

        print(
            "FUEL_NULL_GRAIN:", column, nulls,
            flush=True
        )

        assert nulls == 0

    # 3. Build exactly the same fuel series ID
    # used by dim_fuel_series.
    fuel = (
        fuel
        .withColumn(
            "fuel_series_id",
            F.concat_ws(
                "|",
                "source_family",
                "series_code",
                "region",
                "unit"
            )
        )
        .withColumn(
            "fuel_fact_id",
            F.concat_ws(
                "||",
                "source_family",
                "series_code",
                "region",
                F.date_format(
                    "quote_date", "yyyy-MM-dd"
                ),
                "unit"
            )
        )
    )

    # 4. Resolve dimension keys.
    joined = (
        fuel.alias("f")
        .join(
            dates.alias("d"),
            F.col("f.quote_date")
            == F.col("d.full_date"),
            "left"
        )
        .join(
            sources.alias("s"),
            F.col("f.source_family")
            == F.col("s.source_code"),
            "left"
        )
        .join(
            series.alias("p"),
            F.col("f.fuel_series_id")
            == F.col("p.fuel_series_id"),
            "left"
        )
    )

    joined_total = joined.count()

    print(
        "FUEL_JOINED_ROWS:", joined_total,
        flush=True
    )

    assert joined_total == input_total

    for key in [
        "date_key",
        "source_key",
        "fuel_series_key"
    ]:
        unmatched = joined.filter(
            F.col(key).isNull()
        ).count()

        print(
            "UNMATCHED_FK:", key, unmatched,
            flush=True
        )

        assert unmatched == 0

    # 5. Build Fact.
    fact = joined.select(
        stable_hash("f.fuel_fact_id").alias(
            "fuel_fact_key"
        ),

        F.col("d.date_key").cast("int").alias(
            "date_key"
        ),

        F.col("s.source_key").cast("bigint").alias(
            "source_key"
        ),

        F.col("p.fuel_series_key").cast(
            "bigint"
        ).alias("fuel_series_key"),

        F.col("f.price_vnd_per_unit").cast(
            "decimal(18,2)"
        ).alias("price_vnd_per_unit"),

        F.col("f.price_change_percent").cast(
            "decimal(12,3)"
        ).alias("price_change_percent"),

        F.col("f.price_jump_review").alias(
            "price_jump_review"
        ),

        F.col("f.quality_status").alias(
            "quality_status"
        ),

        F.col("f.raw_observation_count").cast(
            "bigint"
        ).alias("raw_observation_count"),

        F.col("f.distinct_price_count").cast(
            "bigint"
        ).alias("distinct_price_count"),

        F.col("f.min_price").cast(
            "decimal(18,2)"
        ).alias("min_price"),

        F.col("f.max_price").cast(
            "decimal(18,2)"
        ).alias("max_price"),

        F.col("f.selected_quoted_at").alias(
            "selected_quoted_at"
        ),

        F.col("f.selected_bronze_record_id").alias(
            "selected_bronze_record_id"
        ),

        F.col("f.selected_bronze_object_key").alias(
            "selected_bronze_object_key"
        ),

        F.current_timestamp().alias(
            "gold_processed_at"
        )
    ).cache()

    # 6. Validate the Fact.
    total = fact.count()

    unique_keys = fact.select(
        "fuel_fact_key"
    ).distinct().count()

    null_keys = fact.filter(
        F.col("fuel_fact_key").isNull()
        | F.col("date_key").isNull()
        | F.col("source_key").isNull()
        | F.col("fuel_series_key").isNull()
    ).count()

    null_lineage = fact.filter(
        F.col("selected_bronze_record_id").isNull()
        | F.col("selected_bronze_object_key").isNull()
    ).count()

    print("FACT_FUEL_PRICE_ROWS:", total, flush=True)
    print(
        "FACT_FUEL_PRICE_UNIQUE_KEYS:",
        unique_keys,
        flush=True
    )
    print("FACT_FUEL_NULL_KEYS:", null_keys, flush=True)
    print(
        "FACT_FUEL_NULL_LINEAGE:", null_lineage,
        flush=True
    )

    assert total == input_total == unique_keys
    assert null_keys == 0
    assert null_lineage == 0

    # 7. Preserve Silver quality classifications.
    print("FACT_FUEL_QUALITY_STATUS:", flush=True)

    fact.groupBy("quality_status").count().show(
        truncate=False
    )

    expected_quality = {
        "PASS": 1638,
        "PRICE_CONFLICT_REVIEW": 24,
        "PRICE_JUMP_REVIEW": 42
    }

    observed_quality = {
        row["quality_status"]: row["count"]
        for row in fact.groupBy(
            "quality_status"
        ).count().collect()
    }

    assert observed_quality == expected_quality, (
        "Fuel quality distribution differs from Silver"
    )

    print("FACT_FUEL_BY_SOURCE:", flush=True)

    fuel.groupBy("source_family").count().show(
        truncate=False
    )

    print("FACT_FUEL_PRICE_VALIDATED", flush=True)

    # 8. Write only with --write.
    if WRITE:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                fuel_fact_key BIGINT,
                date_key INT,
                source_key BIGINT,
                fuel_series_key BIGINT,
                price_vnd_per_unit DECIMAL(18,2),
                price_change_percent DECIMAL(12,3),
                price_jump_review BOOLEAN,
                quality_status STRING,
                raw_observation_count BIGINT,
                distinct_price_count BIGINT,
                min_price DECIMAL(18,2),
                max_price DECIMAL(18,2),
                selected_quoted_at TIMESTAMP,
                selected_bronze_record_id STRING,
                selected_bronze_object_key STRING,
                gold_processed_at TIMESTAMP
            )
            USING iceberg
        """)

        if spark.table(TARGET).columns != fact.columns:
            raise RuntimeError(
                "Fact Fuel schema mismatch"
            )

        fact.createOrReplaceTempView(
            "stage_fact_fuel_price"
        )

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_fuel_price s
            ON t.fuel_fact_key = s.fuel_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        saved = spark.table(TARGET)

        actual = saved.count()
        actual_unique = saved.select(
            "fuel_fact_key"
        ).distinct().count()

        print(
            "FACT_FUEL_PRICE_ICEBERG_TOTAL:",
            actual,
            flush=True
        )

        assert actual == total == actual_unique

        print(
            "FACT_FUEL_PRICE_ICEBERG_PASSED",
            flush=True
        )

    else:
        print(
            "FACT_FUEL_PRICE_DRY_RUN_PASSED",
            flush=True
        )
        print(
            "NO_GOLD_FACT_WRITTEN",
            flush=True
        )

finally:
    spark.stop()
