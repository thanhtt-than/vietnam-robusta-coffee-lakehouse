
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


WRITE = "--write" in sys.argv[1:]

SILVER = "lakehouse.silver.coffee_psd_observations"
DIM_GEO = "lakehouse.gold.dim_geography"
DIM_YEAR = "lakehouse.gold.dim_year"
DIM_SOURCE = "lakehouse.gold.dim_source"
TARGET = "lakehouse.gold.fact_psd"

SOURCE_CODE = "usda_psd"

GRAIN = [
    "geography_key",
    "commodity_code",
    "market_year_start",
    "calendar_year",
    "month",
    "attribute_id",
    "unit_id"
]

FACT_COLUMNS = [
    "psd_fact_key",
    "geography_key",
    "market_year_key",
    "calendar_year_key",
    "source_key",
    "country_code",
    "country_name",
    "commodity_code",
    "commodity_description",
    "market_year",
    "market_year_start",
    "market_year_end",
    "calendar_year",
    "month",
    "period_type",
    "attribute_id",
    "attribute_description",
    "unit_id",
    "unit_description",
    "value",
    "raw_value",
    "quality_status",
    "silver_record_id",
    "bronze_record_id",
    "bronze_object_key",
    "bronze_record_position",
    "source_filename",
    "object_sha256",
    "silver_processed_at",
    "gold_processed_at"
]

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-FACT-PSD")
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


def assert_zero(df, label):
    count = df.count()

    print(f"{label}: {count}", flush=True)

    if count > 0:
        df.show(20, truncate=False)
        raise RuntimeError(f"{label} validation failed")


try:
    print("===== BUILD FACT_PSD =====", flush=True)

    # 1. Read LONG_CSV only.
    source_all = (
        spark.table(SILVER)
        .filter(F.col("source_format") == "LONG_CSV")
    )

    source_rows = source_all.count()

    print("PSD_LONG_SOURCE_ROWS:", source_rows, flush=True)

    required = [
        "silver_record_id",
        "commodity_code",
        "commodity_description",
        "country_name",
        "market_year_start",
        "calendar_year",
        "month",
        "attribute_id",
        "attribute_description",
        "unit_id",
        "unit_description",
        "value",
        "quality_status",
        "source_filename",
        "bronze_object_key",
        "bronze_record_id"
    ]

    missing = set(required) - set(source_all.columns)

    if missing:
        raise RuntimeError(
            f"Missing Silver columns: {sorted(missing)}"
        )

    invalid_condition = (
        F.col("quality_status").isNull()
        | (F.col("quality_status") != "PASS")
    )

    for column in required:
        invalid_condition = (
            invalid_condition
            | F.col(column).isNull()
        )

    assert_zero(
        source_all.filter(invalid_condition),
        "PSD_INVALID_SOURCE_ROWS"
    )

    # 2. Validate Silver record IDs.
    silver_unique = (
        source_all.select("silver_record_id")
        .distinct()
        .count()
    )

    print("PSD_SILVER_UNIQUE_IDS:", silver_unique, flush=True)

    if silver_unique != source_rows:
        raise RuntimeError("Duplicate Silver record IDs")

    # 3. Validate source business grain.
    source_grain = [
        "country_code",
        "commodity_code",
        "market_year_start",
        "calendar_year",
        "month",
        "attribute_id",
        "unit_id"
    ]

    source_duplicates = (
        source_all
        .groupBy(*source_grain)
        .count()
        .filter(F.col("count") > 1)
    )

    assert_zero(
        source_duplicates,
        "PSD_SOURCE_DUPLICATE_GRAIN_GROUPS"
    )

    # 4. Load and validate DIM_GEOGRAPHY.
    geography = spark.table(DIM_GEO)

    geo_lookup = geography.select(
        F.col("geography_key"),
        F.upper(
            F.trim(F.col("geography_name"))
        ).alias("_geo_name_norm")
    )

    assert_zero(
        geo_lookup.groupBy("_geo_name_norm")
        .count()
        .filter(F.col("count") > 1),
        "DIM_GEOGRAPHY_DUPLICATE_NAMES"
    )

    source = source_all.withColumn(
        "_geo_name_norm",
        F.upper(F.trim(F.col("country_name")))
    )

    # Vietnam in USDA PSD maps to Viet Nam in DIM_GEOGRAPHY.
    source = source.withColumn(
        "_geo_name_norm",
        F.when(
            F.col("_geo_name_norm") == "VIETNAM",
            F.lit("VIET NAM")
        ).otherwise(F.col("_geo_name_norm"))
    )

    # 5. Load DIM_YEAR.
    year_dimension = spark.table(DIM_YEAR)

    year_lookup_market = year_dimension.select(
        F.col("calendar_year").alias("_market_year"),
        F.col("year_key").alias("market_year_key")
    )

    year_lookup_calendar = year_dimension.select(
        F.col("calendar_year").alias("_calendar_year"),
        F.col("year_key").alias("calendar_year_key")
    )

    # 6. Load DIM_SOURCE.
    source_dimension = (
        spark.table(DIM_SOURCE)
        .filter(F.col("source_code") == SOURCE_CODE)
    )

    if source_dimension.count() != 1:
        raise RuntimeError(
            "DIM_SOURCE must contain exactly one usda_psd member"
        )

    source_key = source_dimension.first()["source_key"]

    print("PSD_SOURCE_KEY:", source_key, flush=True)

    # 7. Lookup all dimensions.
    mapped = (
        source.alias("s")

        .join(
            geo_lookup.alias("g"),
            F.col("s._geo_name_norm")
            == F.col("g._geo_name_norm"),
            "left"
        )

        .join(
            year_lookup_market.alias("my"),
            F.col("s.market_year_start")
            == F.col("my._market_year"),
            "left"
        )

        .join(
            year_lookup_calendar.alias("cy"),
            F.col("s.calendar_year")
            == F.col("cy._calendar_year"),
            "left"
        )
    )

    assert_zero(
        mapped.filter(F.col("g.geography_key").isNull())
        .select("s.country_code", "s.country_name")
        .distinct(),
        "PSD_UNMAPPED_GEOGRAPHIES"
    )

    assert_zero(
        mapped.filter(
            F.col("my.market_year_key").isNull()
        ).select("s.market_year_start").distinct(),
        "PSD_UNMAPPED_MARKET_YEARS"
    )

    assert_zero(
        mapped.filter(
            F.col("cy.calendar_year_key").isNull()
        ).select("s.calendar_year").distinct(),
        "PSD_UNMAPPED_CALENDAR_YEARS"
    )

    # 8. Build fact records.
    fact = mapped.select(
        F.col("g.geography_key").cast("int")
         .alias("geography_key"),

        F.col("my.market_year_key").cast("int")
         .alias("market_year_key"),

        F.col("cy.calendar_year_key").cast("int")
         .alias("calendar_year_key"),

        F.lit(source_key).cast("long")
         .alias("source_key"),

        F.col("s.country_code").alias("country_code"),
        F.col("s.country_name").alias("country_name"),

        F.col("s.commodity_code").alias("commodity_code"),
        F.col("s.commodity_description")
         .alias("commodity_description"),

        F.col("s.market_year").alias("market_year"),
        F.col("s.market_year_start").cast("int")
         .alias("market_year_start"),
        F.col("s.market_year_end").cast("int")
         .alias("market_year_end"),

        F.col("s.calendar_year").cast("int")
         .alias("calendar_year"),

        F.col("s.month").cast("int").alias("month"),

        F.when(
            F.col("s.month") == 0,
            F.lit("ANNUAL")
        ).otherwise(
            F.lit("MONTHLY")
        ).alias("period_type"),

        F.col("s.attribute_id").alias("attribute_id"),
        F.col("s.attribute_description")
         .alias("attribute_description"),

        F.col("s.unit_id").alias("unit_id"),
        F.col("s.unit_description").alias("unit_description"),

        F.col("s.value").cast(DecimalType(24, 6))
         .alias("value"),

        F.col("s.raw_value").alias("raw_value"),
        F.col("s.quality_status").alias("quality_status"),

        F.col("s.silver_record_id").alias("silver_record_id"),
        F.col("s.bronze_record_id").alias("bronze_record_id"),
        F.col("s.bronze_object_key").alias("bronze_object_key"),

        F.col("s.bronze_record_position")
         .cast("long").alias("bronze_record_position"),

        F.col("s.source_filename").alias("source_filename"),
        F.col("s.object_sha256").alias("object_sha256"),

        F.col("s.silver_processed_at")
         .alias("silver_processed_at"),

        F.current_timestamp().alias("gold_processed_at")
    )

    # 9. Generate deterministic fact key from business grain.
    key_expression = F.concat_ws(
        "||",
        *[
            F.col(column).cast("string")
            for column in GRAIN
        ]
    )

    fact = (
        fact
        .withColumn(
            "psd_fact_key",
            F.sha2(key_expression, 256)
        )
        .select(*FACT_COLUMNS)
        .cache()
    )

    fact_rows = fact.count()

    print("PSD_FACT_ROWS:", fact_rows, flush=True)

    if fact_rows != source_rows:
        raise RuntimeError(
            f"Join changed row count: "
            f"source={source_rows}, fact={fact_rows}"
        )

    # 10. Validate fact grain.
    fact_duplicates = (
        fact.groupBy(*GRAIN)
        .count()
        .filter(F.col("count") > 1)
    )

    assert_zero(
        fact_duplicates,
        "PSD_FACT_DUPLICATE_GRAIN_GROUPS"
    )

    fact_unique_keys = (
        fact.select("psd_fact_key")
        .distinct()
        .count()
    )

    print("PSD_FACT_UNIQUE_KEYS:", fact_unique_keys, flush=True)

    if fact_unique_keys != fact_rows:
        raise RuntimeError("Duplicate FACT_PSD keys")

    # 11. Validate foreign keys.
    assert_zero(
        fact.filter(
            F.col("geography_key").isNull()
            | F.col("market_year_key").isNull()
            | F.col("calendar_year_key").isNull()
            | F.col("source_key").isNull()
        ),
        "PSD_FACT_NULL_FOREIGN_KEYS"
    )

    # 12. Quality summary.
    fact.groupBy("quality_status").count().show()

    fact.groupBy("period_type").count().show()

    fact.groupBy("attribute_id", "attribute_description") \
        .count().orderBy("attribute_id").show(30, False)

    fact.select(
        "psd_fact_key",
        "geography_key",
        "market_year_key",
        "calendar_year_key",
        "country_name",
        "market_year_start",
        "calendar_year",
        "month",
        "attribute_id",
        "value",
        "unit_description",
        "source_key"
    ).show(15, truncate=False)

    print("FACT_PSD_DRY_RUN_VALIDATION_PASSED", flush=True)

    # 13. Write Iceberg only when --write is supplied.
    if WRITE:

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_psd (
                psd_fact_key STRING,
                geography_key INT,
                market_year_key INT,
                calendar_year_key INT,
                source_key BIGINT,
                country_code STRING,
                country_name STRING,
                commodity_code STRING,
                commodity_description STRING,
                market_year STRING,
                market_year_start INT,
                market_year_end INT,
                calendar_year INT,
                month INT,
                period_type STRING,
                attribute_id STRING,
                attribute_description STRING,
                unit_id STRING,
                unit_description STRING,
                value DECIMAL(24,6),
                raw_value STRING,
                quality_status STRING,
                silver_record_id STRING,
                bronze_record_id STRING,
                bronze_object_key STRING,
                bronze_record_position BIGINT,
                source_filename STRING,
                object_sha256 STRING,
                silver_processed_at TIMESTAMP,
                gold_processed_at TIMESTAMP
            )
            USING iceberg
        """)

        existing = spark.table(TARGET)

        if existing.columns != FACT_COLUMNS:
            raise RuntimeError(
                "Existing FACT_PSD schema does not match "
                "the expected schema"
            )

        before_rows = existing.count()

        before_unique = (
            existing.select("psd_fact_key")
            .distinct()
            .count()
        )

        if before_rows != before_unique:
            raise RuntimeError(
                "Existing FACT_PSD contains duplicate keys"
            )

        fact.createOrReplaceTempView("stage_fact_psd")

        new_rows = (
            fact.select("psd_fact_key")
            .join(
                existing.select("psd_fact_key"),
                "psd_fact_key",
                "left_anti"
            )
            .count()
        )

        expected_rows = before_rows + new_rows

        print("FACT_PSD_BEFORE_ROWS:", before_rows, flush=True)
        print("FACT_PSD_NEW_ROWS:", new_rows, flush=True)
        print("FACT_PSD_EXPECTED_ROWS:", expected_rows, flush=True)

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_psd s
            ON t.psd_fact_key = s.psd_fact_key

            WHEN MATCHED THEN
                UPDATE SET *

            WHEN NOT MATCHED THEN
                INSERT *
        """)

        result = spark.table(TARGET)

        actual_rows = result.count()

        actual_unique = (
            result.select("psd_fact_key")
            .distinct()
            .count()
        )

        missing_after_write = (
            fact.select("psd_fact_key")
            .join(
                result.select("psd_fact_key"),
                "psd_fact_key",
                "left_anti"
            )
            .count()
        )

        assert actual_rows == expected_rows
        assert actual_unique == actual_rows
        assert missing_after_write == 0

        print("FACT_PSD_ICEBERG_ROWS:", actual_rows, flush=True)
        print("FACT_PSD_ICEBERG_UNIQUE_KEYS:", actual_unique, flush=True)
        print("FACT_PSD_MISSING_AFTER_WRITE:", missing_after_write, flush=True)

        print("FACT_PSD_ICEBERG_WRITE_PASSED", flush=True)

    else:
        print("NO_GOLD_TABLE_WRITTEN", flush=True)

    print("GOLD_FACT_PSD_PASSED", flush=True)

finally:
    spark.stop()
