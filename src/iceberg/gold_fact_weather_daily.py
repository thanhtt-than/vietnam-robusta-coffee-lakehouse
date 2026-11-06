import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.fact_weather_daily"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-Weather-Daily")
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


def verify_unique(df, columns, name):
    total = df.count()
    unique = df.select(*columns).distinct().count()
    if total != unique:
        raise RuntimeError(
            f"Duplicate business keys in {name}: "
            f"{total} rows vs {unique} keys"
        )


try:
    # 1. Read existing Silver and Gold Dimensions.
    weather = spark.table(
        "lakehouse.silver.weather_daily"
    )

    dates = spark.table(
        "lakehouse.gold.dim_date"
    ).select("date_key", "full_date")

    geography = spark.table(
        "lakehouse.gold.dim_geography"
    ).select("geography_key", "geography_code")

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select("source_key", "source_code")

    locations = spark.table(
        "lakehouse.gold.dim_weather_location"
    ).select(
        "weather_location_key",
        "source_series_id",
        "province_code",
        "source",
        "latitude",
        "longitude"
    )

    input_total = weather.count()
    print("WEATHER_SILVER_TOTAL:", input_total, flush=True)

    assert input_total == 97620

    # 2. Verify source and dimension granularity.
    verify_unique(
        weather,
        ["source_series_id", "observation_date"],
        "silver.weather_daily"
    )
    verify_unique(dates, ["full_date"], "dim_date")
    verify_unique(
        geography, ["geography_code"], "dim_geography"
    )
    verify_unique(sources, ["source_code"], "dim_source")
    verify_unique(
        locations, ["source_series_id"], "dim_weather_location"
    )

    location_count = locations.count()
    print(
        "WEATHER_LOCATION_SERIES:",
        location_count,
        flush=True
    )
    assert location_count == 12

    # 3. Resolve foreign keys.
    joined = (
        weather.alias("w")
        .join(
            dates.alias("d"),
            F.col("w.observation_date")
            == F.col("d.full_date"),
            "left"
        )
        .join(
            geography.alias("g"),
            F.col("w.province")
            == F.col("g.geography_code"),
            "left"
        )
        .join(
            sources.alias("s"),
            F.col("w.source")
            == F.col("s.source_code"),
            "left"
        )
        .join(
            locations.alias("l"),
            F.col("w.source_series_id")
            == F.col("l.source_series_id"),
            "left"
        )
    ).cache()

    joined_count = joined.count()

    print(
        "WEATHER_JOINED_ROWS:",
        joined_count,
        flush=True
    )

    assert joined_count == input_total

    fk_columns = {
        "date_key": "d.date_key",
        "geography_key": "g.geography_key",
        "source_key": "s.source_key",
        "weather_location_key": "l.weather_location_key"
    }

    for name, column in fk_columns.items():
        missing = joined.filter(
            F.col(column).isNull()
        ).count()

        print("UNMATCHED_FK:", name, missing, flush=True)

        assert missing == 0, (
            f"Unmatched dimension key: {name}"
        )

    # 4. Ensure weather series metadata is consistent
    # with the corresponding Gold Dimension.
    metadata_mismatch = joined.filter(
        ~F.col("w.province").eqNullSafe(
            F.col("l.province_code")
        )
        | ~F.col("w.source").eqNullSafe(
            F.col("l.source")
        )
        | ~F.col("w.latitude").eqNullSafe(
            F.col("l.latitude")
        )
        | ~F.col("w.longitude").eqNullSafe(
            F.col("l.longitude")
        )
    ).count()

    print(
        "WEATHER_LOCATION_METADATA_MISMATCH:",
        metadata_mismatch,
        flush=True
    )

    assert metadata_mismatch == 0

    # 5. Build the Gold Fact.
    # Natural grain: source_series_id + observation_date.
    fact = joined.select(
        F.expr("""
            CAST(
                CONV(
                    SUBSTR(
                        SHA2(
                            CONCAT_WS(
                                '||',
                                w.source_series_id,
                                CAST(w.observation_date AS STRING)
                            ),
                            256
                        ),
                        1,
                        15
                    ),
                    16,
                    10
                ) AS BIGINT
            )
        """).alias("weather_fact_key"),

        F.col("d.date_key").cast("int").alias(
            "date_key"
        ),
        F.col("g.geography_key").cast("int").alias(
            "geography_key"
        ),
        F.col("s.source_key").cast("bigint").alias(
            "source_key"
        ),
        F.col("l.weather_location_key").cast(
            "bigint"
        ).alias("weather_location_key"),

        F.col("w.weather_code").cast("int"),
        F.col("w.temperature_2m_mean"),
        F.col("w.temperature_2m_min"),
        F.col("w.temperature_2m_max"),
        F.col("w.rain_sum"),
        F.col("w.precipitation_sum"),
        F.col("w.et0_fao_evapotranspiration"),

        F.col("w.raw_observation_count"),
        F.col("w.distinct_measurement_count"),
        F.col("w.quality_status"),

        F.col("w.selected_bronze_record_id"),
        F.col("w.selected_bronze_object_key"),
        F.col("w.selected_bronze_record_position"),

        F.current_timestamp().alias(
            "gold_processed_at"
        )
    ).cache()

    # 6. Validate the fact grain, lineage and quality.
    total = fact.count()
    unique = fact.select(
        "weather_fact_key"
    ).distinct().count()

    null_keys = fact.filter(
        F.col("weather_fact_key").isNull()
        | F.col("date_key").isNull()
        | F.col("geography_key").isNull()
        | F.col("source_key").isNull()
        | F.col("weather_location_key").isNull()
    ).count()

    null_lineage = fact.filter(
        F.col("selected_bronze_record_id").isNull()
    ).count()

    print("FACT_WEATHER_DAILY_ROWS:", total, flush=True)
    print(
        "FACT_WEATHER_DAILY_UNIQUE_KEYS:",
        unique,
        flush=True
    )
    print("FACT_WEATHER_NULL_KEYS:", null_keys, flush=True)
    print(
        "FACT_WEATHER_NULL_LINEAGE:",
        null_lineage,
        flush=True
    )

    assert total == input_total == unique
    assert null_keys == 0
    assert null_lineage == 0

    print("FACT_WEATHER_QUALITY_STATUS:", flush=True)

    fact.groupBy("quality_status").count().show(
        truncate=False
    )

    print("FACT_WEATHER_BY_LOCATION:", flush=True)

    fact.groupBy(
        "weather_location_key"
    ).count().orderBy(
        "weather_location_key"
    ).show(20, truncate=False)

    print(
        "FACT_WEATHER_DAILY_VALIDATED",
        flush=True
    )

    # 7. Iceberg write is explicitly opt-in.
    if WRITE:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                weather_fact_key BIGINT,
                date_key INT,
                geography_key INT,
                source_key BIGINT,
                weather_location_key BIGINT,
                weather_code INT,
                temperature_2m_mean DOUBLE,
                temperature_2m_min DOUBLE,
                temperature_2m_max DOUBLE,
                rain_sum DOUBLE,
                precipitation_sum DOUBLE,
                et0_fao_evapotranspiration DOUBLE,
                raw_observation_count BIGINT,
                distinct_measurement_count BIGINT,
                quality_status STRING,
                selected_bronze_record_id STRING,
                selected_bronze_object_key STRING,
                selected_bronze_record_position BIGINT,
                gold_processed_at TIMESTAMP
            )
            USING iceberg
        """)

        if spark.table(TARGET).columns != fact.columns:
            raise RuntimeError(
                "Fact Weather schema mismatch"
            )

        fact.createOrReplaceTempView(
            "stage_fact_weather_daily"
        )

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_weather_daily s
            ON t.weather_fact_key = s.weather_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        saved = spark.table(TARGET)

        actual = saved.count()
        actual_unique = saved.select(
            "weather_fact_key"
        ).distinct().count()

        print(
            "FACT_WEATHER_DAILY_ICEBERG_TOTAL:",
            actual,
            flush=True
        )

        assert actual == total == actual_unique

        print(
            "FACT_WEATHER_DAILY_ICEBERG_PASSED",
            flush=True
        )

    else:
        print(
            "FACT_WEATHER_DAILY_DRY_RUN_PASSED",
            flush=True
        )
        print("NO_GOLD_FACT_WRITTEN", flush=True)

finally:
    spark.stop()
