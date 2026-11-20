import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


WRITE = "--write" in sys.argv[1:]

SILVER = "lakehouse.silver.robusta_stock_totals_daily"
PORT_SILVER = "lakehouse.silver.robusta_stock_port_daily"
DIM_DATE = "lakehouse.gold.dim_date"
DIM_SOURCE = "lakehouse.gold.dim_source"
TARGET = "lakehouse.gold.fact_robusta_stock_totals"

SOURCE_CODE = "robusta_stock_report"

GRAIN = ["cutoff_date"]

MEASURE_COLUMNS = [
    "total_lots_with_val_cert",
    "total_lots_non_tend",
    "total_lots_suspended",
    "port_sum_lots_with_val_cert",
    "port_sum_lots_non_tend",
    "port_sum_lots_suspended",
    "delta_lots_with_val_cert",
    "delta_lots_non_tend",
    "delta_lots_suspended",
    "port_count"
]

FACT_COLUMNS = [
    "stock_totals_fact_key", "date_key", "source_key", "cutoff_date",
    "total_lots_with_val_cert", "total_lots_non_tend", "total_lots_suspended",
    "port_sum_lots_with_val_cert", "port_sum_lots_non_tend",
    "port_sum_lots_suspended", "delta_lots_with_val_cert",
    "delta_lots_non_tend", "delta_lots_suspended", "port_count",
    "raw_observation_count", "distinct_values_count",
    "reconciliation_status", "quality_status", "selected_source",
    "selected_source_file", "selected_bronze_object_key",
    "selected_bronze_record_id", "selected_bronze_record_position",
    "selected_object_sha256", "silver_processed_at", "gold_processed_at"
]

REQUIRED_COLUMNS = [
    "cutoff_date", *MEASURE_COLUMNS,
    "raw_observation_count", "distinct_values_count",
    "reconciliation_status", "quality_status", "selected_source",
    "selected_source_file", "selected_bronze_object_key",
    "selected_bronze_record_id", "selected_bronze_record_position",
    "selected_object_sha256", "silver_processed_at"
]

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-FACT-ROBUSTA-STOCK-TOTALS")
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
    print("===== BUILD FACT_ROBUSTA_STOCK_TOTALS =====", flush=True)

    # 1. Read and validate Silver totals source.
    source_all = spark.table(SILVER)
    missing = set(REQUIRED_COLUMNS) - set(source_all.columns)
    if missing:
        raise RuntimeError(f"Missing Silver columns: {sorted(missing)}")

    source_rows = source_all.count()
    print("STOCK_TOTALS_SOURCE_ROWS:", source_rows, flush=True)
    if source_rows == 0:
        raise RuntimeError("Silver stock-totals source is empty")

    assert_zero(
        source_all.filter(
            F.col("selected_source").isNull()
            | (F.col("selected_source") != SOURCE_CODE)
        ),
        "STOCK_TOTALS_UNEXPECTED_SOURCE_ROWS"
    )

    invalid_condition = (
        F.col("cutoff_date").isNull()
        | F.col("quality_status").isNull()
        | (F.col("quality_status") != "PASS")
        | F.col("reconciliation_status").isNull()
        | (F.col("reconciliation_status") != "MATCH")
        | F.col("raw_observation_count").isNull()
        | F.col("distinct_values_count").isNull()
    )
    for column in MEASURE_COLUMNS:
        invalid_condition = invalid_condition | F.col(column).isNull()

    assert_zero(
        source_all.filter(invalid_condition),
        "STOCK_TOTALS_INVALID_SOURCE_ROWS"
    )

    # 2. Validate one totals record per cutoff date.
    source_duplicates = (
        source_all.groupBy(*GRAIN)
        .count()
        .filter(F.col("count") > 1)
    )
    assert_zero(
        source_duplicates,
        "STOCK_TOTALS_SOURCE_DUPLICATE_GRAIN_GROUPS"
    )

    # 3. Reconcile daily totals against port-level Silver data.
    port_source = spark.table(PORT_SILVER)
    port_required = [
        "cutoff_date", "port_id", "commodity",
        "lots_with_val_cert", "lots_non_tend", "lots_suspended"
    ]
    missing_port_columns = set(port_required) - set(port_source.columns)
    if missing_port_columns:
        raise RuntimeError(
            f"Missing port Silver columns: {sorted(missing_port_columns)}"
        )

    port_duplicates = (
        port_source.groupBy("cutoff_date", "commodity", "port_id")
        .count()
        .filter(F.col("count") > 1)
    )
    assert_zero(
        port_duplicates,
        "STOCK_TOTALS_PORT_SOURCE_DUPLICATE_GRAIN_GROUPS"
    )

    port_sum = (
        port_source.groupBy("cutoff_date")
        .agg(
            F.sum("lots_with_val_cert").cast("long")
            .alias("_actual_port_sum_cert"),
            F.sum("lots_non_tend").cast("long")
            .alias("_actual_port_sum_non_tend"),
            F.sum("lots_suspended").cast("long")
            .alias("_actual_port_sum_suspended"),
            F.countDistinct("port_id").cast("long")
            .alias("_actual_port_count")
        )
    )

    reconciliation = (
        source_all.alias("t")
        .join(
            port_sum.alias("p"),
            F.col("t.cutoff_date") == F.col("p.cutoff_date"),
            "full_outer"
        )
    )

    assert_zero(
        reconciliation.filter(F.col("t.cutoff_date").isNull()),
        "STOCK_TOTALS_MISSING_TOTALS_DATES"
    )
    assert_zero(
        reconciliation.filter(F.col("p.cutoff_date").isNull()),
        "STOCK_TOTALS_MISSING_PORT_DATES"
    )

    reconciliation_mismatch = reconciliation.filter(
        (F.col("t.port_sum_lots_with_val_cert")
         != F.col("p._actual_port_sum_cert"))
        | (F.col("t.port_sum_lots_non_tend")
           != F.col("p._actual_port_sum_non_tend"))
        | (F.col("t.port_sum_lots_suspended")
           != F.col("p._actual_port_sum_suspended"))
        | (F.col("t.port_count") != F.col("p._actual_port_count"))
    )
    assert_zero(
        reconciliation_mismatch.select(
            F.col("t.cutoff_date").alias("cutoff_date"),
            F.col("t.port_sum_lots_with_val_cert"),
            F.col("p._actual_port_sum_cert"),
            F.col("t.port_sum_lots_non_tend"),
            F.col("p._actual_port_sum_non_tend"),
            F.col("t.port_sum_lots_suspended"),
            F.col("p._actual_port_sum_suspended"),
            F.col("t.port_count"),
            F.col("p._actual_port_count")
        ),
        "STOCK_TOTALS_PORT_RECONCILIATION_MISMATCHES"
    )

    # 4. Load and validate DIM_DATE.
    date_lookup = spark.table(DIM_DATE).select(
        F.col("date_key").cast("int").alias("date_key"),
        F.col("full_date").alias("_full_date")
    )
    assert_zero(
        date_lookup.groupBy("_full_date")
        .count()
        .filter(F.col("count") > 1),
        "DIM_DATE_DUPLICATE_DATES"
    )

    # 5. Load DIM_SOURCE.
    source_dimension = (
        spark.table(DIM_SOURCE)
        .filter(F.col("source_code") == SOURCE_CODE)
    )
    if source_dimension.count() != 1:
        raise RuntimeError(
            "DIM_SOURCE must contain exactly one robusta_stock_report member"
        )
    source_key = source_dimension.first()["source_key"]
    print("STOCK_TOTALS_SOURCE_KEY:", source_key, flush=True)

    # 6. Lookup dimensions.
    mapped = (
        source_all.alias("s")
        .join(
            date_lookup.alias("d"),
            F.col("s.cutoff_date") == F.col("d._full_date"),
            "left"
        )
    )
    assert_zero(
        mapped.filter(F.col("d.date_key").isNull())
        .select(F.col("s.cutoff_date").alias("cutoff_date"))
        .distinct(),
        "STOCK_TOTALS_UNMAPPED_DATES"
    )

    # 7. Build fact records.
    fact = mapped.select(
        F.col("d.date_key").cast("int").alias("date_key"),
        F.lit(source_key).cast("long").alias("source_key"),
        F.col("s.cutoff_date").alias("cutoff_date"),
        F.col("s.total_lots_with_val_cert").cast("long")
        .alias("total_lots_with_val_cert"),
        F.col("s.total_lots_non_tend").cast("long").alias("total_lots_non_tend"),
        F.col("s.total_lots_suspended").cast("long")
        .alias("total_lots_suspended"),
        F.col("s.port_sum_lots_with_val_cert").cast("long")
        .alias("port_sum_lots_with_val_cert"),
        F.col("s.port_sum_lots_non_tend").cast("long")
        .alias("port_sum_lots_non_tend"),
        F.col("s.port_sum_lots_suspended").cast("long")
        .alias("port_sum_lots_suspended"),
        F.col("s.delta_lots_with_val_cert").cast("long")
        .alias("delta_lots_with_val_cert"),
        F.col("s.delta_lots_non_tend").cast("long")
        .alias("delta_lots_non_tend"),
        F.col("s.delta_lots_suspended").cast("long")
        .alias("delta_lots_suspended"),
        F.col("s.port_count").cast("long").alias("port_count"),
        F.col("s.raw_observation_count").cast("long")
        .alias("raw_observation_count"),
        F.col("s.distinct_values_count").cast("long")
        .alias("distinct_values_count"),
        F.col("s.reconciliation_status").alias("reconciliation_status"),
        F.col("s.quality_status").alias("quality_status"),
        F.col("s.selected_source").alias("selected_source"),
        F.col("s.selected_source_file").alias("selected_source_file"),
        F.col("s.selected_bronze_object_key").alias("selected_bronze_object_key"),
        F.col("s.selected_bronze_record_id").alias("selected_bronze_record_id"),
        F.col("s.selected_bronze_record_position").cast("long")
        .alias("selected_bronze_record_position"),
        F.col("s.selected_object_sha256").alias("selected_object_sha256"),
        F.col("s.silver_processed_at").alias("silver_processed_at"),
        F.current_timestamp().alias("gold_processed_at")
    )

    # 8. Generate deterministic fact key from business grain.
    fact = (
        fact
        .withColumn(
            "stock_totals_fact_key",
            F.sha2(F.col("cutoff_date").cast("string"), 256)
        )
        .select(*FACT_COLUMNS)
        .cache()
    )

    fact_rows = fact.count()
    print("STOCK_TOTALS_FACT_ROWS:", fact_rows, flush=True)
    if fact_rows != source_rows:
        raise RuntimeError(
            f"Join changed row count: source={source_rows}, fact={fact_rows}"
        )

    # 9. Validate fact grain and surrogate key.
    fact_duplicates = (
        fact.groupBy("date_key")
        .count()
        .filter(F.col("count") > 1)
    )
    assert_zero(
        fact_duplicates,
        "STOCK_TOTALS_FACT_DUPLICATE_GRAIN_GROUPS"
    )

    fact_unique_keys = fact.select("stock_totals_fact_key").distinct().count()
    print("STOCK_TOTALS_FACT_UNIQUE_KEYS:", fact_unique_keys, flush=True)
    if fact_unique_keys != fact_rows:
        raise RuntimeError("Duplicate FACT_ROBUSTA_STOCK_TOTALS keys")

    # 10. Validate foreign keys.
    assert_zero(
        fact.filter(
            F.col("date_key").isNull()
            | F.col("source_key").isNull()
        ),
        "STOCK_TOTALS_FACT_NULL_FOREIGN_KEYS"
    )

    # 11. Quality summary and sample.
    fact.groupBy("quality_status", "reconciliation_status").count().show()
    fact.select(
        "stock_totals_fact_key", "date_key", "source_key", "cutoff_date",
        "total_lots_with_val_cert", "total_lots_non_tend",
        "total_lots_suspended", "port_count",
        "reconciliation_status", "quality_status"
    ).show(15, truncate=False)

    print("FACT_ROBUSTA_STOCK_TOTALS_DRY_RUN_PASSED", flush=True)

    # 12. Write Iceberg only when --write is supplied.
    if WRITE:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_robusta_stock_totals (
                stock_totals_fact_key STRING,
                date_key INT,
                source_key BIGINT,
                cutoff_date DATE,
                total_lots_with_val_cert BIGINT,
                total_lots_non_tend BIGINT,
                total_lots_suspended BIGINT,
                port_sum_lots_with_val_cert BIGINT,
                port_sum_lots_non_tend BIGINT,
                port_sum_lots_suspended BIGINT,
                delta_lots_with_val_cert BIGINT,
                delta_lots_non_tend BIGINT,
                delta_lots_suspended BIGINT,
                port_count BIGINT,
                raw_observation_count BIGINT,
                distinct_values_count BIGINT,
                reconciliation_status STRING,
                quality_status STRING,
                selected_source STRING,
                selected_source_file STRING,
                selected_bronze_object_key STRING,
                selected_bronze_record_id STRING,
                selected_bronze_record_position BIGINT,
                selected_object_sha256 STRING,
                silver_processed_at TIMESTAMP,
                gold_processed_at TIMESTAMP
            )
            USING iceberg
        """)

        existing = spark.table(TARGET)
        if existing.columns != FACT_COLUMNS:
            raise RuntimeError(
                "Existing FACT_ROBUSTA_STOCK_TOTALS schema "
                "does not match the expected schema"
            )

        before_rows = existing.count()
        before_unique = existing.select("stock_totals_fact_key").distinct().count()
        if before_rows != before_unique:
            raise RuntimeError(
                "Existing FACT_ROBUSTA_STOCK_TOTALS contains duplicate keys"
            )

        fact.createOrReplaceTempView("stage_fact_robusta_stock_totals")
        new_rows = (
            fact.select("stock_totals_fact_key")
            .join(
                existing.select("stock_totals_fact_key"),
                "stock_totals_fact_key",
                "left_anti"
            )
            .count()
        )
        expected_rows = before_rows + new_rows

        print("STOCK_TOTALS_BEFORE_ROWS:", before_rows, flush=True)
        print("STOCK_TOTALS_NEW_ROWS:", new_rows, flush=True)
        print("STOCK_TOTALS_EXPECTED_ROWS:", expected_rows, flush=True)

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_fact_robusta_stock_totals s
            ON t.stock_totals_fact_key = s.stock_totals_fact_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        result = spark.table(TARGET)
        actual_rows = result.count()
        actual_unique = result.select("stock_totals_fact_key").distinct().count()
        missing_after_write = (
            fact.select("stock_totals_fact_key")
            .join(
                result.select("stock_totals_fact_key"),
                "stock_totals_fact_key",
                "left_anti"
            )
            .count()
        )

        assert actual_rows == expected_rows
        assert actual_unique == actual_rows
        assert missing_after_write == 0

        print("STOCK_TOTALS_ICEBERG_ROWS:", actual_rows, flush=True)
        print("STOCK_TOTALS_ICEBERG_UNIQUE_KEYS:", actual_unique, flush=True)
        print("STOCK_TOTALS_MISSING_AFTER_WRITE:", missing_after_write, flush=True)
        print("FACT_ROBUSTA_STOCK_TOTALS_ICEBERG_WRITE_PASSED", flush=True)
    else:
        print("NO_GOLD_TABLE_WRITTEN", flush=True)

    print("GOLD_FACT_ROBUSTA_STOCK_TOTALS_PASSED", flush=True)

finally:
    spark.stop()
