import sys
from pyspark.sql import SparkSession, functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.fact_production"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-Production")
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


def unique_count(df, col):
    return df.select(col).distinct().count()


try:
    # 1. Read the two annual Silver datasets.
    area = spark.table(
        "lakehouse.silver.coffee_area_annual"
    )

    production = spark.table(
        "lakehouse.silver.coffee_production_annual"
    )

    years = spark.table(
        "lakehouse.gold.dim_year"
    ).select("year_key", "calendar_year")

    geography = spark.table(
        "lakehouse.gold.dim_geography"
    ).select("geography_key", "geography_code")

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select("source_key", "source_code")

    area_total = area.count()
    production_total = production.count()

    print("AREA_SILVER_TOTAL:", area_total, flush=True)
    print(
        "PRODUCTION_SILVER_TOTAL:",
        production_total,
        flush=True
    )

    assert area_total == 19
    assert production_total == 19

    # 2. Check natural keys and domain.
    for name, df in [
        ("area", area),
        ("production", production)
    ]:
        assert unique_count(df, "year") == df.count()
        assert df.filter(
            F.col("year").isNull()
        ).count() == 0

        assert df.filter(
            F.col("year") == 2006
        ).count() == 0

    for name, df, key in [
        ("dim_year", years, "calendar_year"),
        ("dim_geography", geography, "geography_code"),
        ("dim_source", sources, "source_code")
    ]:
        assert unique_count(df, key) == df.count(), (
            "Duplicate key in " + name
        )

    # The two annual sources should cover the same years.
    missing_in_production = area.select(
        "year"
    ).join(
        production.select("year"),
        "year",
        "left_anti"
    ).count()

    missing_in_area = production.select(
        "year"
    ).join(
        area.select("year"),
        "year",
        "left_anti"
    ).count()

    print(
        "AREA_YEARS_WITHOUT_PRODUCTION:",
        missing_in_production,
        flush=True
    )
    print(
        "PRODUCTION_YEARS_WITHOUT_AREA:",
        missing_in_area,
        flush=True
    )

    assert missing_in_production == 0
    assert missing_in_area == 0

    # 3. Join two measures strictly on calendar year.
    combined = (
        area.alias("a")
        .join(
            production.alias("p"),
            F.col("a.year") == F.col("p.year"),
            "inner"
        )
        .join(
            years.alias("y"),
            F.col("a.year")
            == F.col("y.calendar_year"),
            "left"
        )
    )

    # This Fact is nationwide, not provincial.
    vn = geography.filter(
        F.col("geography_code") == "VN"
    )

    assert vn.count() == 1

    combined = (
        combined
        .crossJoin(vn.alias("g"))
        .join(
            sources.alias("sa"),
            F.col("a.selected_source")
            == F.col("sa.source_code"),
            "left"
        )
        .join(
            sources.alias("sp"),
            F.col("p.selected_source")
            == F.col("sp.source_code"),
            "left"
        )
    ).cache()

    joined_total = combined.count()

    print(
        "PRODUCTION_JOINED_ROWS:",
        joined_total,
        flush=True
    )

    assert joined_total == 19

    # 4. Validate dimension coverage.
    for label, column in [
        ("year_key", "y.year_key"),
        ("geography_key", "g.geography_key"),
        ("area_source_key", "sa.source_key"),
        ("production_source_key", "sp.source_key")
    ]:
        missing = combined.filter(
            F.col(column).isNull()
        ).count()

        print(
            "UNMATCHED_FK:", label, missing,
            flush=True
        )

        assert missing == 0, (
            "Unmatched dimension: " + label
        )

    # 5. Build the Fact.
    fact = combined.select(
        F.col("y.year_key").cast("bigint").alias(
            "production_fact_key"
        ),
        F.col("y.year_key").cast("int").alias(
            "year_key"
        ),
        F.col("g.geography_key").cast("int").alias(
            "geography_key"
        ),

        F.col("sa.source_key").cast("bigint").alias(
            "area_source_key"
        ),
        F.col("sp.source_key").cast("bigint").alias(
            "production_source_key"
        ),

        F.col("a.coffee_area_thousand_ha").alias(
            "coffee_area_thousand_ha"
        ),
        F.col(
            "p.coffee_production_thousand_tons"
        ).alias(
            "coffee_production_thousand_tons"
        ),

        F.col(
            "a.growth_index_prev_year_100"
        ).alias("area_growth_index_prev_year_100"),

        F.col(
            "p.growth_index_prev_year_100"
        ).alias("production_growth_index_prev_year_100"),

        F.col("a.publication_status").alias(
            "area_publication_status"
        ),
        F.col("p.publication_status").alias(
            "production_publication_status"
        ),

        F.col("a.quality_status").alias(
            "area_quality_status"
        ),
        F.col("p.quality_status").alias(
            "production_quality_status"
        ),

        F.col("a.raw_observation_count").alias(
            "area_raw_observation_count"
        ),
        F.col("p.raw_observation_count").alias(
            "production_raw_observation_count"
        ),

        F.col("a.selected_bronze_record_id").alias(
            "area_bronze_record_id"
        ),
        F.col("p.selected_bronze_record_id").alias(
            "production_bronze_record_id"
        ),

        F.col("a.selected_bronze_object_key").alias(
            "area_bronze_object_key"
        ),
        F.col("p.selected_bronze_object_key").alias(
            "production_bronze_object_key"
        ),

        F.current_timestamp().alias(
            "gold_processed_at"
        )
    ).cache()

    # Align the four decimal columns with the existing Gold schema.
    decimal_columns = [
        "coffee_area_thousand_ha",
        "coffee_production_thousand_tons",
        "area_growth_index_prev_year_100",
        "production_growth_index_prev_year_100"
    ]

    for column in decimal_columns:
        fact = fact.withColumn(
            column,
            F.col(column).cast("decimal(18,3)")
        )

    # Prevent silent overflow or precision loss for required measures.
    for column in decimal_columns[:2]:
        assert fact.filter(F.col(column).isNull()).count() == 0
    # 6. Verify production fact quality.
    total = fact.count()
    unique = unique_count(
        fact, "production_fact_key"
    )

    null_keys = fact.filter(
        F.col("production_fact_key").isNull()
        | F.col("year_key").isNull()
        | F.col("geography_key").isNull()
        | F.col("area_source_key").isNull()
        | F.col("production_source_key").isNull()
    ).count()

    null_measures = fact.filter(
        F.col("coffee_area_thousand_ha").isNull()
        | F.col(
            "coffee_production_thousand_tons"
        ).isNull()
    ).count()

    null_lineage = fact.filter(
        F.col("area_bronze_record_id").isNull()
        | F.col("production_bronze_record_id").isNull()
        | F.col("area_bronze_object_key").isNull()
        | F.col("production_bronze_object_key").isNull()
    ).count()

    missing_2006 = fact.filter(
        F.col("year_key") == 2006
    ).count()

    print("FACT_PRODUCTION_ROWS:", total, flush=True)
    print(
        "FACT_PRODUCTION_UNIQUE_KEYS:",
        unique,
        flush=True
    )
    print(
        "FACT_PRODUCTION_NULL_KEYS:",
        null_keys,
        flush=True
    )
    print(
        "FACT_PRODUCTION_NULL_MEASURES:",
        null_measures,
        flush=True
    )
    print(
        "FACT_PRODUCTION_NULL_LINEAGE:",
        null_lineage,
        flush=True
    )
    print(
        "FACT_PRODUCTION_YEAR_2006_ROWS:",
        missing_2006,
        flush=True
    )

    assert total == unique == 19
    assert null_keys == 0
    assert null_measures == 0
    assert null_lineage == 0
    assert missing_2006 == 0

    fact.orderBy("year_key").select(
        "year_key",
        "geography_key",
        "coffee_area_thousand_ha",
        "coffee_production_thousand_tons",
        "area_quality_status",
        "production_quality_status"
    ).show(25, truncate=False)

    print(
        "FACT_PRODUCTION_VALIDATED",
        flush=True
    )

    # 7. Publish only after all checks pass.
    if WRITE:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                production_fact_key BIGINT,
                year_key INT,
                geography_key INT,
                area_source_key BIGINT,
                production_source_key BIGINT,
                coffee_area_thousand_ha DECIMAL(18,3),
                coffee_production_thousand_tons DECIMAL(18,3),
                area_growth_index_prev_year_100 DECIMAL(18,3),
                production_growth_index_prev_year_100 DECIMAL(18,3),
                area_publication_status STRING,
                production_publication_status STRING,
                area_quality_status STRING,
                production_quality_status STRING,
                area_raw_observation_count BIGINT,
                production_raw_observation_count BIGINT,
                area_bronze_record_id STRING,
                production_bronze_record_id STRING,
                area_bronze_object_key STRING,
                production_bronze_object_key STRING,
                gold_processed_at TIMESTAMP
            )
            USING iceberg
        """)

        target_columns = spark.table(TARGET).columns

        if target_columns != fact.columns:
            raise RuntimeError(
                "Production Fact schema mismatch"
            )

        # Verify Spark data types too, not only column names.
        target_types = [
            x.dataType for x in spark.table(TARGET).schema
        ]
        stage_types = [
            x.dataType for x in fact.schema
        ]

        if target_types != stage_types:
            raise RuntimeError(
                "Production Fact data type mismatch: "
                + str(
                    list(zip(
                        fact.columns, stage_types, target_types
                    ))
                )
            )

        fact.createOrReplaceTempView(
            "stage_fact_production"
        )

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_production s
            ON t.production_fact_key = s.production_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        saved = spark.table(TARGET)
        actual = saved.count()
        actual_unique = unique_count(
            saved, "production_fact_key"
        )

        print(
            "FACT_PRODUCTION_ICEBERG_TOTAL:",
            actual,
            flush=True
        )

        assert actual == total == actual_unique

        print(
            "FACT_PRODUCTION_ICEBERG_PASSED",
            flush=True
        )

    else:
        print(
            "FACT_PRODUCTION_DRY_RUN_PASSED",
            flush=True
        )
        print(
            "NO_GOLD_FACT_WRITTEN",
            flush=True
        )

finally:
    spark.stop()

