import sys
from pyspark.sql import SparkSession, functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.fact_export"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-Export")
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
    print("KEY_CHECK:", name, total, unique, flush=True)
    assert total == unique, "Duplicate keys: " + name


try:
    trade = spark.table(
        "lakehouse.silver.coffee_trade_annual"
    )

    years = spark.table(
        "lakehouse.gold.dim_year"
    ).select("year_key", "calendar_year")

    geo = spark.table(
        "lakehouse.gold.dim_geography"
    ).select("geography_key", "geography_code")

    destination = spark.table(
        "lakehouse.gold.dim_destination"
    ).select(
        "destination_key",
        "partner_code",
        "is_world_aggregate"
    )

    products = spark.table(
        "lakehouse.gold.dim_hs_product"
    ).select("hs_product_key", "product_code")

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select("source_key", "source_code")

    input_total = trade.count()
    print("EXPORT_SILVER_TOTAL:", input_total, flush=True)
    assert input_total == 3888

    verify_unique(
        trade,
        ["reporter_code", "partner_code", "product_code", "year"],
        "silver.coffee_trade_annual"
    )
    verify_unique(years, ["calendar_year"], "dim_year")
    verify_unique(geo, ["geography_code"], "dim_geography")
    verify_unique(
        destination, ["partner_code"], "dim_destination"
    )
    verify_unique(products, ["product_code"], "dim_hs_product")
    verify_unique(sources, ["source_code"], "dim_source")

    # Silver currently contains Viet Nam as sole reporter.
    reporter_codes = [
        r["reporter_code"]
        for r in trade.select("reporter_code").distinct().collect()
    ]
    print("EXPORT_REPORTER_CODES:", reporter_codes, flush=True)
    assert reporter_codes == ["704"]

    for col in [
        "reporter_code", "partner_code", "product_code",
        "year", "selected_source"
    ]:
        assert trade.filter(F.col(col).isNull()).count() == 0

    vn = geo.filter(F.col("geography_code") == "VN")
    assert vn.count() == 1

    joined = (
        trade.alias("t")
        .join(
            years.alias("y"),
            F.col("t.year") == F.col("y.calendar_year"),
            "left"
        )
        .crossJoin(vn.alias("g"))
        .join(
            destination.alias("d"),
            F.col("t.partner_code") == F.col("d.partner_code"),
            "left"
        )
        .join(
            products.alias("h"),
            F.col("t.product_code") == F.col("h.product_code"),
            "left"
        )
        .join(
            sources.alias("s"),
            F.col("t.selected_source") == F.col("s.source_code"),
            "left"
        )
    ).cache()

    joined_total = joined.count()
    print("EXPORT_JOINED_ROWS:", joined_total, flush=True)
    assert joined_total == input_total

    for name, column in [
        ("year_key", "y.year_key"),
        ("geography_key", "g.geography_key"),
        ("destination_key", "d.destination_key"),
        ("hs_product_key", "h.hs_product_key"),
        ("source_key", "s.source_key")
    ]:
        missing = joined.filter(F.col(column).isNull()).count()
        print("UNMATCHED_FK:", name, missing, flush=True)
        assert missing == 0

    world_mismatch = joined.filter(
        ~F.col("t.is_world_aggregate").eqNullSafe(
            F.col("d.is_world_aggregate")
        )
    ).count()

    print("EXPORT_WORLD_FLAG_MISMATCH:", world_mismatch, flush=True)
    assert world_mismatch == 0

    # Stable key: reporter + partner + product + year.
    business_id = F.concat_ws(
        "||",
        F.col("t.reporter_code"),
        F.col("t.partner_code"),
        F.col("t.product_code"),
        F.col("t.year").cast("string")
    )

    fact = joined.select(
        F.conv(
            F.substring(F.sha2(business_id, 256), 1, 15),
            16, 10
        ).cast("bigint").alias("export_fact_key"),

        F.col("y.year_key").cast("int").alias("year_key"),
        F.col("g.geography_key").cast("int").alias("geography_key"),
        F.col("d.destination_key").cast("bigint").alias(
            "destination_key"
        ),
        F.col("h.hs_product_key").cast("bigint").alias(
            "hs_product_key"
        ),
        F.col("s.source_key").cast("bigint").alias("source_key"),

        F.col("t.reporter_code").alias("reporter_code"),
        F.col("t.is_world_aggregate").alias(
            "is_world_aggregate"
        ),
        F.col("t.value_usd_thousand").cast(
            "decimal(20,3)"
        ).alias("value_usd_thousand"),

        F.col("t.quality_status").alias("quality_status"),
        F.col("t.raw_observation_count").cast(
            "bigint"
        ).alias("raw_observation_count"),
        F.col("t.distinct_values_count").cast(
            "bigint"
        ).alias("distinct_values_count"),

        F.col("t.selected_bronze_record_id").alias(
            "selected_bronze_record_id"
        ),
        F.col("t.selected_bronze_object_key").alias(
            "selected_bronze_object_key"
        ),
        F.col("t.selected_bronze_record_position").cast(
            "bigint"
        ).alias("selected_bronze_record_position"),

        F.current_timestamp().alias("gold_processed_at")
    ).cache()

    total = fact.count()
    unique = fact.select("export_fact_key").distinct().count()

    print("FACT_EXPORT_ROWS:", total, flush=True)
    print("FACT_EXPORT_UNIQUE_KEYS:", unique, flush=True)
    assert total == unique == 3888

    required = [
        "export_fact_key", "year_key", "geography_key",
        "destination_key", "hs_product_key", "source_key",
        "value_usd_thousand", "selected_bronze_record_id",
        "selected_bronze_object_key"
    ]

    for column in required:
        missing = fact.filter(F.col(column).isNull()).count()
        print("NULL_REQUIRED:", column, missing, flush=True)
        assert missing == 0

    world_rows = fact.filter(
        F.col("is_world_aggregate") == True
    ).count()

    partner_rows = fact.filter(
        F.col("is_world_aggregate") == False
    ).count()

    world_years = (
        fact.filter(F.col("is_world_aggregate") == True)
        .select("year_key").distinct().count()
    )

    print("FACT_EXPORT_WORLD_ROWS:", world_rows, flush=True)
    print("FACT_EXPORT_PARTNER_ROWS:", partner_rows, flush=True)
    print("FACT_EXPORT_WORLD_YEARS:", world_years, flush=True)

    assert world_rows == 24
    assert partner_rows == 3864
    assert world_years == 24

    print("FACT_EXPORT_QUALITY_STATUS:", flush=True)
    fact.groupBy("quality_status").count().show(
        truncate=False
    )

    print("FACT_EXPORT_VALIDATED", flush=True)

    if WRITE:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                export_fact_key BIGINT,
                year_key INT,
                geography_key INT,
                destination_key BIGINT,
                hs_product_key BIGINT,
                source_key BIGINT,
                reporter_code STRING,
                is_world_aggregate BOOLEAN,
                value_usd_thousand DECIMAL(20,3),
                quality_status STRING,
                raw_observation_count BIGINT,
                distinct_values_count BIGINT,
                selected_bronze_record_id STRING,
                selected_bronze_object_key STRING,
                selected_bronze_record_position BIGINT,
                gold_processed_at TIMESTAMP
            ) USING iceberg
        """)

        existing = spark.table(TARGET)

        if existing.columns != fact.columns:
            raise RuntimeError("Fact Export column mismatch")

        existing_types = [
            f.dataType for f in existing.schema
        ]
        fact_types = [
            f.dataType for f in fact.schema
        ]

        if existing_types != fact_types:
            raise RuntimeError(
                "Fact Export data type mismatch: "
                + str(list(zip(
                    fact.columns, fact_types, existing_types
                )))
            )

        fact.createOrReplaceTempView("stage_fact_export")

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_export s
            ON t.export_fact_key = s.export_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        saved = spark.table(TARGET)
        actual = saved.count()
        actual_unique = (
            saved.select("export_fact_key").distinct().count()
        )

        print(
            "FACT_EXPORT_ICEBERG_TOTAL:",
            actual, flush=True
        )

        assert actual == total == actual_unique

        print("FACT_EXPORT_ICEBERG_PASSED", flush=True)

    else:
        print("FACT_EXPORT_DRY_RUN_PASSED", flush=True)
        print("NO_GOLD_FACT_WRITTEN", flush=True)

finally:
    spark.stop()
