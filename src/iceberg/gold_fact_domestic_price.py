import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.fact_domestic_price"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Fact-Domestic-Price")
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


def check_unique(df, columns, name):
    total = df.count()
    unique = df.select(*columns).distinct().count()

    if total != unique:
        raise RuntimeError(
            f"Duplicate keys in {name}: {total} vs {unique}"
        )


try:
    # 1. Read Silver and Gold Dimensions.
    coffee = spark.table(
        "lakehouse.silver.coffee_prices_daily"
    )

    dates = spark.table(
        "lakehouse.gold.dim_date"
    ).select(
        "date_key", "full_date"
    )

    geography = spark.table(
        "lakehouse.gold.dim_geography"
    ).select(
        "geography_key", "geography_code"
    )

    sources = spark.table(
        "lakehouse.gold.dim_source"
    ).select(
        "source_key", "source_code"
    )

    series = spark.table(
        "lakehouse.gold.dim_price_series"
    ).select(
        "price_series_key", "series_code"
    )

    # 2. Prevent many-to-many joins.
    check_unique(dates, ["full_date"], "dim_date")
    check_unique(geography, ["geography_code"], "dim_geography")
    check_unique(sources, ["source_code"], "dim_source")
    check_unique(series, ["series_code"], "dim_price_series")

    input_count = coffee.count()
    print("COFFEE_SILVER_TOTAL:", input_count, flush=True)

    if input_count == 0:
        raise RuntimeError("Silver coffee_prices_daily is empty; refusing to build Fact")

    # Silver grain: one row per source_family + province + quote_date.
    source_grain = ["source_family", "province", "quote_date"]
    check_unique(coffee, source_grain, "silver.coffee_prices_daily")

    null_source_keys = coffee.filter(
        F.col("source_family").isNull()
        | F.col("province").isNull()
        | F.col("quote_date").isNull()
    ).count()

    if null_source_keys:
        raise RuntimeError(
            f"Silver coffee has {null_source_keys} rows with null business keys"
        )

    # Stable natural identity for source/province/date.
    coffee = (
        coffee
        .withColumn(
            "series_code",
            F.concat_ws("|", "source_family", "province")
        )
        .withColumn(
            "price_fact_id",
            F.concat_ws(
                "|",
                "source_family",
                "province",
                F.date_format("quote_date", "yyyy-MM-dd")
            )
        )
    )

    # 3. Join to Dimensions, preserving all rows.
    joined = (
        coffee.alias("c")
        .join(
            dates.alias("d"),
            F.col("c.quote_date") == F.col("d.full_date"),
            "left"
        )
        .join(
            geography.alias("g"),
            F.col("c.province") == F.col("g.geography_code"),
            "left"
        )
        .join(
            sources.alias("s"),
            F.col("c.source_family") == F.col("s.source_code"),
            "left"
        )
        .join(
            series.alias("p"),
            F.col("c.series_code") == F.col("p.series_code"),
            "left"
        )
    )

    # 4. Check dimension coverage before writing.
    fk_columns = [
        "date_key",
        "geography_key",
        "source_key",
        "price_series_key"
    ]

    for key in fk_columns:
        missing = joined.filter(F.col(key).isNull()).count()
        print("UNMATCHED_FK:", key, missing, flush=True)

        if missing != 0:
            raise RuntimeError(f"Unmatched Gold dimension: {key}")

    # 5. Build Fact with deterministic primary key.
    fact = joined.select(
        F.expr(
            "CAST(CONV(SUBSTR(SHA2(price_fact_id, 256), "
            "1, 15), 16, 10) AS BIGINT)"
        ).alias("price_fact_key"),

        F.col("date_key").cast("int"),
        F.col("geography_key").cast("int"),
        F.col("source_key").cast("bigint"),
        F.col("price_series_key").cast("bigint"),

        F.col("price_vnd_kg").cast("decimal(18,2)"),
        F.col("quality_status"),
        F.col("raw_observation_count").cast("bigint"),
        F.col("distinct_price_count").cast("bigint"),
        F.col("min_price_vnd_kg").cast("decimal(18,2)"),
        F.col("max_price_vnd_kg").cast("decimal(18,2)"),

        F.col("selected_bronze_record_id"),
        F.col("selected_bronze_object_key"),

        F.current_timestamp().alias("gold_processed_at")
    ).cache()

    # 6. Validate the Fact grain and row preservation.
    total = fact.count()
    unique = fact.select("price_fact_key").distinct().count()

    print("FACT_DOMESTIC_PRICE_ROWS:", total, flush=True)
    print("FACT_DOMESTIC_PRICE_UNIQUE_KEYS:", unique, flush=True)

    if total != input_count:
        raise RuntimeError(
            f"Join changed row count: Silver={input_count}, Fact={total}"
        )
    if unique != total:
        raise RuntimeError(
            f"Fact primary-key collision or duplicate: rows={total}, unique={unique}"
        )

    null_pk = fact.filter(F.col("price_fact_key").isNull()).count()
    null_lineage = fact.filter(
        F.col("selected_bronze_record_id").isNull()
    ).count()

    if null_pk:
        raise RuntimeError(f"Fact contains {null_pk} null primary keys")
    if null_lineage:
        raise RuntimeError(f"Fact contains {null_lineage} rows without Bronze lineage")

    # Do not freeze the number of conflicts: new ingestion can legitimately
    # introduce new conflict-review rows. Validate the allowed status domain.
    print("FACT_QUALITY_STATUS:", flush=True)
    fact.groupBy("quality_status").count().show(truncate=False)

    invalid_quality = fact.filter(
        F.col("quality_status").isNull()
        | ~F.col("quality_status").isin("PASS", "PRICE_CONFLICT_REVIEW")
    ).count()

    if invalid_quality:
        raise RuntimeError(
            f"Fact contains {invalid_quality} rows with unknown quality_status"
        )

    print("FACT_SAMPLE:", flush=True)
    fact.orderBy(
        "date_key", "geography_key", "source_key"
    ).show(8, truncate=False)

    print("FACT_DOMESTIC_PRICE_VALIDATED", flush=True)

    if WRITE:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                price_fact_key BIGINT,
                date_key INT,
                geography_key INT,
                source_key BIGINT,
                price_series_key BIGINT,
                price_vnd_kg DECIMAL(18,2),
                quality_status STRING,
                raw_observation_count BIGINT,
                distinct_price_count BIGINT,
                min_price_vnd_kg DECIMAL(18,2),
                max_price_vnd_kg DECIMAL(18,2),
                selected_bronze_record_id STRING,
                selected_bronze_object_key STRING,
                gold_processed_at TIMESTAMP
            )
            USING iceberg
        """)

        if spark.table(TARGET).columns != fact.columns:
            raise RuntimeError("Gold Fact schema mismatch")

        fact.createOrReplaceTempView("stage_fact_domestic_price")

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_domestic_price s
            ON t.price_fact_key = s.price_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        stored = spark.table(TARGET)
        actual = stored.count()
        actual_unique = stored.select("price_fact_key").distinct().count()

        source_keys = fact.select("price_fact_key").distinct()
        stored_keys = stored.select("price_fact_key").distinct()

        missing_source_keys = source_keys.join(
            stored_keys,
            on="price_fact_key",
            how="left_anti"
        ).count()

        retained_existing_keys = stored_keys.join(
            source_keys,
            on="price_fact_key",
            how="left_anti"
        ).count()

        print("FACT_DOMESTIC_PRICE_ICEBERG_TOTAL:", actual, flush=True)
        print("FACT_DOMESTIC_PRICE_SOURCE_TOTAL:", total, flush=True)
        print("FACT_DOMESTIC_PRICE_ICEBERG_UNIQUE_KEYS:", actual_unique, flush=True)
        print("FACT_DOMESTIC_PRICE_MISSING_SOURCE_KEYS:", missing_source_keys, flush=True)
        print("FACT_DOMESTIC_PRICE_RETAINED_EXISTING_KEYS:", retained_existing_keys, flush=True)

        if missing_source_keys:
            raise RuntimeError(
                f"Gold Fact is missing {missing_source_keys} current source keys after MERGE"
            )
        if actual != actual_unique:
            raise RuntimeError(
                f"Gold Fact contains duplicate primary keys: rows={actual}, unique={actual_unique}"
            )
        if actual < total:
            raise RuntimeError(
                f"Gold Fact has fewer rows ({actual}) than current source ({total})"
            )

        print("FACT_DOMESTIC_PRICE_ICEBERG_PASSED", flush=True)

    else:
        print("FACT_DOMESTIC_PRICE_DRY_RUN_PASSED", flush=True)
        print("NO_GOLD_FACT_WRITTEN", flush=True)

finally:
    spark.stop()
