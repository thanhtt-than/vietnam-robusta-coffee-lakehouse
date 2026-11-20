"""
TLCN Silver Iceberg: Robusta port stock and daily grand totals.

Source:
    lakehouse.bronze.robusta_stock_raw

Targets:
    lakehouse.silver.robusta_stock_port_daily
    lakehouse.silver.robusta_stock_totals_daily

Default mode is READ-ONLY dry-run. Use --write only after reviewing the
quality and reconciliation output. Bronze is never modified.
"""
import argparse

from pyspark.sql import SparkSession, Window, functions as F


BRONZE = "lakehouse.bronze.robusta_stock_raw"
SILVER_NAMESPACE = "lakehouse.silver"

PORT_TARGET = f"{SILVER_NAMESPACE}.robusta_stock_port_daily"
TOTAL_TARGET = f"{SILVER_NAMESPACE}.robusta_stock_totals_daily"

MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

METRICS = [
    "lots_with_val_cert",
    "lots_non_tend",
    "lots_suspended",
]


def make_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Silver-Robusta-Stock")
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
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def map_value(field):
    return F.trim(F.element_at(F.col("_payload"), F.lit(field)))


def parse_cutoff_date(raw_column):
    """
    Parse both source formats:
      - 16-Nov-2025 -> 2025-11-16
      - 13-May-26   -> 2026-05-13

    For this Robusta stock report series, two-digit years are 20xx.
    Do not derive CutOffDate from ingestion_date or filename when the
    source field is present.
    """
    value = F.upper(F.trim(raw_column))

    day_text = F.regexp_extract(
        value, r"^([0-9]{1,2})-[A-Z]{3}-(?:[0-9]{4}|[0-9]{2})$", 1
    )
    month_text = F.regexp_extract(
        value, r"^[0-9]{1,2}-([A-Z]{3})-(?:[0-9]{4}|[0-9]{2})$", 1
    )
    long_year_text = F.regexp_extract(
        value, r"^[0-9]{1,2}-[A-Z]{3}-([0-9]{4})$", 1
    )
    short_year_text = F.regexp_extract(
        value, r"^[0-9]{1,2}-[A-Z]{3}-([0-9]{2})$", 1
    )

    month_map = F.create_map(*[
        item
        for month, number in MONTHS.items()
        for item in (F.lit(month), F.lit(number))
    ])

    day = F.when(day_text != "", day_text.cast("int"))
    month = F.element_at(month_map, month_text)
    year = (
        F.when(long_year_text != "", long_year_text.cast("int"))
         .when(short_year_text != "", short_year_text.cast("int") + F.lit(2000))
    )

    return F.when(
        day.isNotNull() & month.isNotNull() & year.isNotNull(),
        F.make_date(year, month, day)
    )


def parse_lots(raw_column):
    """Parse integer lot counts; invalid values remain NULL, never become zero."""
    cleaned = F.regexp_replace(F.trim(raw_column), ",", "")
    return F.when(
        cleaned.rlike(r"^[0-9]+$"),
        cleaned.cast("long")
    ).otherwise(F.lit(None).cast("long"))


def canonicalize(df, keys, value_columns):
    """
    One Silver row per business key.
    Prefer the lexically latest source filename, then deterministic lineage.
    Conflicting source versions remain visible through quality flags/counts.
    """
    fingerprint_values = [
        F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
        for c in value_columns
    ]
    prepared = df.withColumn(
        "_value_fingerprint",
        F.sha2(F.to_json(F.array(*fingerprint_values)), 256)
    )

    stats = prepared.groupBy(*keys).agg(
        F.count("*").alias("raw_observation_count"),
        F.countDistinct("_value_fingerprint").alias("distinct_values_count")
    )

    window = Window.partitionBy(*keys).orderBy(
        F.col("source_file").desc_nulls_last(),
        F.col("object_key").desc_nulls_last(),
        F.col("record_position").desc_nulls_last(),
        F.col("record_id").desc_nulls_last()
    )

    chosen = (
        prepared
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "_value_fingerprint")
    )

    return chosen.join(stats, keys, "inner")


def build_source_frames(spark):
    raw = spark.table(BRONZE)

    required_columns = {
        "record_id", "object_key", "record_position", "batch_id",
        "ingestion_date", "domain", "source", "source_file",
        "record_type", "object_sha256", "raw_record_json"
    }
    missing = required_columns - set(raw.columns)
    if missing:
        raise RuntimeError(f"Bronze schema is missing columns: {sorted(missing)}")

    total_rows = raw.count()
    unique_record_ids = raw.select("record_id").distinct().count()

    print(f"BRONZE_ROWS: {total_rows}", flush=True)
    print(f"BRONZE_UNIQUE_RECORD_IDS: {unique_record_ids}", flush=True)

    if total_rows != unique_record_ids:
        raise RuntimeError("Bronze record_id is not unique; no Silver output built")

    invalid_record_types = raw.filter(
        ~F.col("record_type").isin("port_stock", "grand_total")
    ).count()
    if invalid_record_types:
        raise RuntimeError(
            f"Unexpected Bronze record_type rows: {invalid_record_types}"
        )

    parsed = raw.withColumn(
        "_payload",
        F.from_json("raw_record_json", "MAP<STRING,STRING>")
    )

    common = [
        "record_id", "object_key", "record_position", "batch_id",
        "ingestion_date", "domain", "source", "source_file",
        "object_sha256"
    ]

    port_raw = (
        parsed.filter(F.col("record_type") == "port_stock")
        .select(
            *common,
            map_value("Commodity").alias("commodity"),
            parse_cutoff_date(map_value("CutOffDate")).alias("cutoff_date"),
            map_value("PortId").alias("port_id"),
            parse_lots(map_value("LotsWithValCert")).alias("lots_with_val_cert"),
            parse_lots(map_value("LotsNonTend")).alias("lots_non_tend"),
            parse_lots(map_value("LotsSuspended")).alias("lots_suspended"),
            F.col("raw_record_json")
        )
    )

    total_raw = (
        parsed.filter(F.col("record_type") == "grand_total")
        .select(
            *common,
            map_value("Commodity").alias("commodity"),
            parse_lots(map_value("LotsWithValCert")).alias("lots_with_val_cert"),
            parse_lots(map_value("LotsNonTend")).alias("lots_non_tend"),
            parse_lots(map_value("LotsSuspended")).alias("lots_suspended"),
            F.col("raw_record_json")
        )
    )

    # Grand-total source rows do not carry CutOffDate. Derive it from the
    # port rows belonging to the same original CSV object.
    object_dates = port_raw.groupBy("object_key").agg(
        F.countDistinct("cutoff_date").alias("object_cutoff_date_count"),
        F.min("cutoff_date").alias("object_cutoff_date")
    )

    bad_object_dates = object_dates.filter(
        (F.col("object_cutoff_date_count") != 1) |
        F.col("object_cutoff_date").isNull()
    ).count()

    if bad_object_dates:
        object_dates.filter(
            (F.col("object_cutoff_date_count") != 1) |
            F.col("object_cutoff_date").isNull()
        ).show(20, truncate=False)
        raise RuntimeError(
            f"{bad_object_dates} source objects do not have exactly one valid "
            "CutOffDate across port rows"
        )

    total_raw = (
        total_raw
        .join(object_dates.select("object_key", "object_cutoff_date"),
              on="object_key", how="left")
        .withColumnRenamed("object_cutoff_date", "cutoff_date")
    )

    return raw, port_raw, total_raw


def build_port_silver(port_raw):
    invalid = port_raw.filter(
        F.col("cutoff_date").isNull() |
        F.col("port_id").isNull() | (F.trim("port_id") == "") |
        F.col("commodity").isNull() | (F.trim("commodity") == "") |
        F.col("lots_with_val_cert").isNull() |
        F.col("lots_non_tend").isNull() |
        F.col("lots_suspended").isNull() |
        (F.col("lots_with_val_cert") < 0) |
        (F.col("lots_non_tend") < 0) |
        (F.col("lots_suspended") < 0)
    ).count()

    print(f"PORT_STOCK_BRONZE_ROWS: {port_raw.count()}", flush=True)
    print(f"PORT_STOCK_INVALID_REQUIRED_ROWS: {invalid}", flush=True)

    if invalid:
        port_raw.filter(
            F.col("cutoff_date").isNull() |
            F.col("port_id").isNull() | (F.trim("port_id") == "") |
            F.col("lots_with_val_cert").isNull() |
            F.col("lots_non_tend").isNull() |
            F.col("lots_suspended").isNull() |
            (F.col("lots_with_val_cert") < 0) |
            (F.col("lots_non_tend") < 0) |
            (F.col("lots_suspended") < 0)
        ).select(
            "source_file", "record_position", "raw_record_json"
        ).show(20, truncate=False)
        raise RuntimeError("Invalid port stock rows; no Silver writes permitted")

    if port_raw.filter(F.col("commodity") != "RC").count():
        raise RuntimeError("Unexpected commodity in port_stock; expected RC")

    canonical = canonicalize(
        port_raw,
        ["cutoff_date", "port_id"],
        ["commodity", *METRICS]
    )

    result = canonical.select(
        "cutoff_date",
        "commodity",
        "port_id",
        *METRICS,
        "raw_observation_count",
        "distinct_values_count",
        F.when(F.col("distinct_values_count") > 1, "SOURCE_CONFLICT_REVIEW")
         .otherwise("PASS").alias("quality_status"),
        F.col("source").alias("selected_source"),
        F.col("source_file").alias("selected_source_file"),
        F.col("object_key").alias("selected_bronze_object_key"),
        F.col("record_id").alias("selected_bronze_record_id"),
        F.col("record_position").alias("selected_bronze_record_position"),
        F.col("object_sha256").alias("selected_object_sha256"),
        F.current_timestamp().alias("silver_processed_at")
    ).cache()

    count = result.count()
    unique = result.select("cutoff_date", "port_id").distinct().count()

    print(f"PORT_STOCK_SILVER_ROWS: {count}", flush=True)
    print(f"PORT_STOCK_UNIQUE_KEYS: {unique}", flush=True)
    print("PORT_STOCK_QUALITY_COUNTS:", flush=True)
    result.groupBy("quality_status").count().show(truncate=False)

    if count != unique:
        raise RuntimeError("Duplicate Silver port stock business keys remain")

    return result


def build_total_silver(total_raw, port_silver):
    invalid = total_raw.filter(
        F.col("cutoff_date").isNull() |
        (F.col("commodity") != "GrandTotal") |
        F.col("lots_with_val_cert").isNull() |
        F.col("lots_non_tend").isNull() |
        F.col("lots_suspended").isNull() |
        (F.col("lots_with_val_cert") < 0) |
        (F.col("lots_non_tend") < 0) |
        (F.col("lots_suspended") < 0)
    ).count()

    print(f"GRAND_TOTAL_BRONZE_ROWS: {total_raw.count()}", flush=True)
    print(f"GRAND_TOTAL_INVALID_REQUIRED_ROWS: {invalid}", flush=True)

    if invalid:
        total_raw.filter(
            F.col("cutoff_date").isNull() |
            F.col("lots_with_val_cert").isNull() |
            F.col("lots_non_tend").isNull() |
            F.col("lots_suspended").isNull() |
            (F.col("lots_with_val_cert") < 0) |
            (F.col("lots_non_tend") < 0) |
            (F.col("lots_suspended") < 0)
        ).select("source_file", "raw_record_json").show(20, truncate=False)
        raise RuntimeError("Invalid grand total rows; no Silver writes permitted")

    canonical = canonicalize(
        total_raw,
        ["cutoff_date"],
        ["commodity", *METRICS]
    )

    # Reconcile canonical per-port sums with the source grand total.
    port_sums = port_silver.groupBy("cutoff_date").agg(
        F.sum("lots_with_val_cert").alias("port_sum_lots_with_val_cert"),
        F.sum("lots_non_tend").alias("port_sum_lots_non_tend"),
        F.sum("lots_suspended").alias("port_sum_lots_suspended"),
        F.countDistinct("port_id").alias("port_count")
    )

    result = (
        canonical
        .join(port_sums, on="cutoff_date", how="left")
        .withColumn(
            "delta_lots_with_val_cert",
            F.col("port_sum_lots_with_val_cert") - F.col("lots_with_val_cert")
        )
        .withColumn(
            "delta_lots_non_tend",
            F.col("port_sum_lots_non_tend") - F.col("lots_non_tend")
        )
        .withColumn(
            "delta_lots_suspended",
            F.col("port_sum_lots_suspended") - F.col("lots_suspended")
        )
        .withColumn(
            "reconciliation_status",
            F.when(F.col("port_count").isNull(), "NO_PORT_ROWS")
             .when(
                 (F.col("delta_lots_with_val_cert") == 0) &
                 (F.col("delta_lots_non_tend") == 0) &
                 (F.col("delta_lots_suspended") == 0),
                 "MATCH"
             )
             .otherwise("MISMATCH_REVIEW")
        )
        .withColumn(
            "quality_status",
            F.when(F.col("distinct_values_count") > 1, "SOURCE_CONFLICT_REVIEW")
             .when(F.col("reconciliation_status") != "MATCH", "RECONCILIATION_REVIEW")
             .otherwise("PASS")
        )
        .select(
            "cutoff_date",
            F.col("lots_with_val_cert").alias("total_lots_with_val_cert"),
            F.col("lots_non_tend").alias("total_lots_non_tend"),
            F.col("lots_suspended").alias("total_lots_suspended"),
            "port_sum_lots_with_val_cert",
            "port_sum_lots_non_tend",
            "port_sum_lots_suspended",
            "delta_lots_with_val_cert",
            "delta_lots_non_tend",
            "delta_lots_suspended",
            "port_count",
            "raw_observation_count",
            "distinct_values_count",
            "reconciliation_status",
            "quality_status",
            F.col("source").alias("selected_source"),
            F.col("source_file").alias("selected_source_file"),
            F.col("object_key").alias("selected_bronze_object_key"),
            F.col("record_id").alias("selected_bronze_record_id"),
            F.col("record_position").alias("selected_bronze_record_position"),
            F.col("object_sha256").alias("selected_object_sha256"),
            F.current_timestamp().alias("silver_processed_at")
        )
        .cache()
    )

    count = result.count()
    unique = result.select("cutoff_date").distinct().count()

    print(f"STOCK_TOTALS_SILVER_ROWS: {count}", flush=True)
    print(f"STOCK_TOTALS_UNIQUE_DATES: {unique}", flush=True)
    print("STOCK_TOTALS_RECONCILIATION_COUNTS:", flush=True)
    result.groupBy("reconciliation_status").count().show(truncate=False)
    print("STOCK_TOTALS_QUALITY_COUNTS:", flush=True)
    result.groupBy("quality_status").count().show(truncate=False)
    print("STOCK_TOTALS_SAMPLE:", flush=True)
    result.orderBy("cutoff_date").show(10, truncate=False)

    if count != unique:
        raise RuntimeError("Duplicate Silver stock total dates remain")

    return result


def write_iceberg(spark, target, df, keys):
    """
    Idempotent insert-only MERGE.
    Existing rows with the same business key but different logical values
    cause a hard stop instead of being silently overwritten.
    """
    view = "stage_" + target.split(".")[-1]
    df.createOrReplaceTempView(view)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} "
        f"USING iceberg AS SELECT * FROM {view} WHERE 1 = 0"
    )

    existing = spark.table(target)
    if set(existing.columns) != set(df.columns):
        raise RuntimeError(
            f"{target} schema differs from the current Silver output; "
            "no write attempted"
        )

    join_condition = " AND ".join(
        f"t.`{key}` <=> s.`{key}`" for key in keys
    )

    overlap = existing.alias("t").join(
        df.alias("s"),
        on=F.expr(join_condition),
        how="inner"
    )

    logical_columns = [c for c in df.columns if c != "silver_processed_at"]
    mismatch_condition = None
    for column in logical_columns:
        term = ~F.col(f"t.{column}").eqNullSafe(F.col(f"s.{column}"))
        mismatch_condition = (
            term if mismatch_condition is None
            else mismatch_condition | term
        )

    mismatches = overlap.filter(mismatch_condition).count()
    if mismatches:
        raise RuntimeError(
            f"{target} has {mismatches} existing business keys with "
            "different logical values. Inspect before updating."
        )

    spark.sql(f"""
        MERGE INTO {target} t
        USING {view} s
        ON {join_condition}
        WHEN NOT MATCHED THEN INSERT *
    """)

    stored = spark.table(target)
    total = stored.count()
    unique = stored.select(*keys).distinct().count()
    expected = df.count()

    print(f"{target.split('.')[-1].upper()}_ICEBERG_TOTAL: {total}", flush=True)
    print(f"{target.split('.')[-1].upper()}_ICEBERG_UNIQUE_KEYS: {unique}", flush=True)

    if total != unique or total != expected:
        raise RuntimeError(
            f"{target}: stored={total}, unique={unique}, expected={expected}"
        )

    print(f"{target.split('.')[-1].upper()}_ICEBERG_PASSED", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the validated outputs to Silver Iceberg"
    )
    args = parser.parse_args()

    spark = make_spark()
    try:
        raw, port_raw, total_raw = build_source_frames(spark)

        print("===== BRONZE INPUT DISTRIBUTION =====", flush=True)
        raw.groupBy("record_type").count().orderBy("record_type").show(truncate=False)

        port_silver = build_port_silver(port_raw)
        total_silver = build_total_silver(total_raw, port_silver)

        print("===== SILVER DATE COVERAGE =====", flush=True)
        port_silver.agg(
            F.min("cutoff_date").alias("first_cutoff_date"),
            F.max("cutoff_date").alias("last_cutoff_date"),
            F.countDistinct("cutoff_date").alias("distinct_cutoff_dates"),
            F.countDistinct("port_id").alias("distinct_ports")
        ).show(truncate=False)

        print("SILVER_ROBUSTA_STOCK_DRY_RUN_PASSED", flush=True)

        if args.write:
            write_iceberg(
                spark, PORT_TARGET, port_silver,
                ["cutoff_date", "port_id"]
            )
            write_iceberg(
                spark, TOTAL_TARGET, total_silver,
                ["cutoff_date"]
            )
            print("SILVER_ROBUSTA_STOCK_ICEBERG_PASSED", flush=True)
        else:
            print("NO_SILVER_TABLES_WRITTEN", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
