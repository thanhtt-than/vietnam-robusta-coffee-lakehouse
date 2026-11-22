"""Build Gold Data Mart for Robusta certified/non-tenderable/suspended stocks.

Sources:
  lakehouse.gold.fact_robusta_stock_port
  lakehouse.gold.fact_robusta_stock_totals

Dimensions:
  lakehouse.gold.dim_date
  lakehouse.gold.dim_port
  lakehouse.gold.dim_source

Target:
  lakehouse.gold.mart_robusta_stock_analysis

Default mode: dry run only.
Write to Iceberg only when --write is supplied.

Grain:
  One row per cutoff_date + port_key + commodity + source_key.

Important:
  Total-level measures from fact_robusta_stock_totals are repeated on each
  port row for comparison and share calculations. Do not SUM those repeated
  total-level columns across ports.
"""

import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


FACT_PORT = "lakehouse.gold.fact_robusta_stock_port"
FACT_TOTALS = "lakehouse.gold.fact_robusta_stock_totals"
DIM_DATE = "lakehouse.gold.dim_date"
DIM_PORT = "lakehouse.gold.dim_port"
DIM_SOURCE = "lakehouse.gold.dim_source"
TARGET = "lakehouse.gold.mart_robusta_stock_analysis"
WRITE = "--write" in sys.argv[1:]


def create_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Build-Mart-Robusta-Stock-Analysis")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def require_table(spark, table_name):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError("Required table does not exist: " + table_name)


def assert_unique(df, keys, label):
    total = df.count()
    unique = df.select(*keys).distinct().count()
    print(f"{label}_ROWS={total}; {label}_UNIQUE_KEYS={unique}", flush=True)

    if total != unique:
        (
            df.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
            .show(20, truncate=False)
        )
        raise RuntimeError(f"{label}: duplicate key {keys}")


def require_columns(df, columns, label):
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def build_mart(spark):
    for table_name in (
        FACT_PORT,
        FACT_TOTALS,
        DIM_DATE,
        DIM_PORT,
        DIM_SOURCE,
    ):
        require_table(spark, table_name)

    port_fact = spark.table(FACT_PORT)
    totals_fact = spark.table(FACT_TOTALS)
    dates = spark.table(DIM_DATE)
    ports = spark.table(DIM_PORT)
    sources = spark.table(DIM_SOURCE)

    require_columns(
        port_fact,
        [
            "stock_port_fact_key", "date_key", "port_key", "source_key",
            "cutoff_date", "port_id", "commodity", "lots_with_val_cert",
            "lots_non_tend", "lots_suspended", "raw_observation_count",
            "distinct_values_count", "quality_status", "selected_source",
            "selected_source_file", "selected_bronze_object_key",
            "selected_bronze_record_id", "selected_bronze_record_position",
            "selected_object_sha256", "silver_processed_at", "gold_processed_at",
        ],
        "FACT_ROBUSTA_STOCK_PORT",
    )
    require_columns(
        totals_fact,
        [
            "stock_totals_fact_key", "date_key", "source_key", "cutoff_date",
            "total_lots_with_val_cert", "total_lots_non_tend",
            "total_lots_suspended", "port_sum_lots_with_val_cert",
            "port_sum_lots_non_tend", "port_sum_lots_suspended",
            "delta_lots_with_val_cert", "delta_lots_non_tend",
            "delta_lots_suspended", "port_count", "raw_observation_count",
            "distinct_values_count", "reconciliation_status", "quality_status",
            "selected_source", "selected_source_file",
            "selected_bronze_object_key", "selected_bronze_record_id",
            "selected_bronze_record_position", "selected_object_sha256",
            "silver_processed_at", "gold_processed_at",
        ],
        "FACT_ROBUSTA_STOCK_TOTALS",
    )
    require_columns(dates, ["date_key", "full_date", "year", "month", "quarter"], "DIM_DATE")
    require_columns(ports, ["port_key", "port_id"], "DIM_PORT")
    require_columns(sources, ["source_key", "source_code", "source_name"], "DIM_SOURCE")

    # Dimension keys must be unique before joining.
    assert_unique(dates, ["date_key"], "DIM_DATE")
    assert_unique(ports, ["port_key"], "DIM_PORT")
    assert_unique(sources, ["source_key"], "DIM_SOURCE")

    # Validate the natural grain of both source facts before any joins.
    port_grain = ["cutoff_date", "port_key", "commodity", "source_key"]
    totals_grain = ["cutoff_date", "source_key"]
    assert_unique(port_fact, ["stock_port_fact_key"], "FACT_STOCK_PORT_SURROGATE")
    assert_unique(port_fact, port_grain, "FACT_STOCK_PORT_NATURAL_GRAIN")
    assert_unique(totals_fact, ["stock_totals_fact_key"], "FACT_STOCK_TOTALS_SURROGATE")
    assert_unique(totals_fact, totals_grain, "FACT_STOCK_TOTALS_NATURAL_GRAIN")

    null_keys = port_fact.filter(
        F.col("cutoff_date").isNull()
        | F.col("port_key").isNull()
        | F.col("source_key").isNull()
        | F.col("commodity").isNull()
    ).count()
    if null_keys:
        raise RuntimeError(f"FACT_STOCK_PORT has {null_keys} rows with NULL grain keys")

    invalid_port_lots = port_fact.filter(
        (F.col("lots_with_val_cert") < 0)
        | (F.col("lots_non_tend") < 0)
        | (F.col("lots_suspended") < 0)
    ).count()
    if invalid_port_lots:
        raise RuntimeError(f"FACT_STOCK_PORT has {invalid_port_lots} negative stock values")

    invalid_total_lots = totals_fact.filter(
        (F.col("total_lots_with_val_cert") < 0)
        | (F.col("total_lots_non_tend") < 0)
        | (F.col("total_lots_suspended") < 0)
        | (F.col("port_sum_lots_with_val_cert") < 0)
        | (F.col("port_sum_lots_non_tend") < 0)
        | (F.col("port_sum_lots_suspended") < 0)
    ).count()
    if invalid_total_lots:
        raise RuntimeError(f"FACT_STOCK_TOTALS has {invalid_total_lots} negative stock values")

    # Keep total-level values at their own date/source grain before joining.
    totals = totals_fact.select(
        "cutoff_date",
        "source_key",
        F.col("stock_totals_fact_key").alias("stock_totals_fact_key"),
        F.col("total_lots_with_val_cert").alias("total_lots_with_val_cert"),
        F.col("total_lots_non_tend").alias("total_lots_non_tend"),
        F.col("total_lots_suspended").alias("total_lots_suspended"),
        F.col("port_sum_lots_with_val_cert").alias("port_sum_lots_with_val_cert"),
        F.col("port_sum_lots_non_tend").alias("port_sum_lots_non_tend"),
        F.col("port_sum_lots_suspended").alias("port_sum_lots_suspended"),
        F.col("delta_lots_with_val_cert").alias("delta_lots_with_val_cert"),
        F.col("delta_lots_non_tend").alias("delta_lots_non_tend"),
        F.col("delta_lots_suspended").alias("delta_lots_suspended"),
        F.col("port_count").alias("reported_port_count"),
        F.col("reconciliation_status").alias("reconciliation_status"),
        F.col("quality_status").alias("totals_quality_status"),
        F.col("selected_source").alias("totals_selected_source"),
        F.col("selected_source_file").alias("totals_selected_source_file"),
        F.col("selected_bronze_object_key").alias("totals_bronze_object_key"),
        F.col("selected_bronze_record_id").alias("totals_bronze_record_id"),
        F.col("selected_object_sha256").alias("totals_object_sha256"),
    )

    # Join dimensions and the matching total snapshot using both date and source.
    base = (
        port_fact.alias("p")
        .join(
            dates.select("date_key", "full_date", "year", "month", "quarter").alias("d"),
            F.col("p.date_key") == F.col("d.date_key"),
            "left",
        )
        .join(
            ports.select("port_key", "port_id").alias("pt"),
            F.col("p.port_key") == F.col("pt.port_key"),
            "left",
        )
        .join(
            sources.select("source_key", "source_code", "source_name").alias("s"),
            F.col("p.source_key") == F.col("s.source_key"),
            "left",
        )
        .join(
            totals.alias("t"),
            (F.col("p.cutoff_date") == F.col("t.cutoff_date"))
            & (F.col("p.source_key") == F.col("t.source_key")),
            "left",
        )
        .select(
            F.col("p.stock_port_fact_key"),
            F.col("t.stock_totals_fact_key"),
            F.col("p.date_key"),
            F.col("p.cutoff_date"),
            F.col("d.year").alias("stock_year"),
            F.col("d.month").alias("stock_month"),
            F.col("d.quarter").alias("stock_quarter"),
            F.col("p.port_key"),
            F.col("pt.port_id").alias("port_id"),
            F.col("p.port_id").alias("fact_port_id"),
            F.col("p.commodity"),
            F.col("p.source_key"),
            F.col("s.source_code"),
            F.col("s.source_name"),
            F.col("p.lots_with_val_cert"),
            F.col("p.lots_non_tend"),
            F.col("p.lots_suspended"),
            F.col("p.raw_observation_count"),
            F.col("p.distinct_values_count"),
            F.col("p.quality_status").alias("port_quality_status"),
            F.col("p.selected_source"),
            F.col("p.selected_source_file"),
            F.col("p.selected_bronze_object_key"),
            F.col("p.selected_bronze_record_id"),
            F.col("p.selected_bronze_record_position"),
            F.col("p.selected_object_sha256"),
            F.col("p.silver_processed_at"),
            F.col("p.gold_processed_at"),
            F.col("t.total_lots_with_val_cert"),
            F.col("t.total_lots_non_tend"),
            F.col("t.total_lots_suspended"),
            F.col("t.port_sum_lots_with_val_cert"),
            F.col("t.port_sum_lots_non_tend"),
            F.col("t.port_sum_lots_suspended"),
            F.col("t.delta_lots_with_val_cert"),
            F.col("t.delta_lots_non_tend"),
            F.col("t.delta_lots_suspended"),
            F.col("t.reported_port_count"),
            F.col("t.reconciliation_status"),
            F.col("t.totals_quality_status"),
            F.col("t.totals_selected_source"),
            F.col("t.totals_selected_source_file"),
            F.col("t.totals_bronze_object_key"),
            F.col("t.totals_bronze_record_id"),
            F.col("t.totals_object_sha256"),
        )
    )

    source_rows = port_fact.count()
    mart_rows = base.count()
    print(f"STOCK_PORT_FACT_ROWS={source_rows}", flush=True)
    print(f"STOCK_MART_BASE_ROWS={mart_rows}", flush=True)

    if mart_rows != source_rows:
        raise RuntimeError(
            f"JOIN changed row count: source={source_rows}, mart={mart_rows}"
        )

    missing_dimension_refs = base.filter(
        F.col("stock_year").isNull()
        | F.col("port_id").isNull()
        | F.col("source_code").isNull()
        | F.col("source_name").isNull()
    ).count()
    if missing_dimension_refs:
        raise RuntimeError(
            f"Missing Dimension references: {missing_dimension_refs}"
        )

    port_id_mismatches = base.filter(
        F.col("port_id") != F.col("fact_port_id")
    ).count()
    if port_id_mismatches:
        raise RuntimeError(f"DIM_PORT vs Fact port_id mismatches: {port_id_mismatches}")

    # The date dimension is joined by date_key. Validate its full_date separately.
    date_check = (
        port_fact.alias("p")
        .join(
            dates.select(
                F.col("date_key").alias("d_date_key"),
                F.col("full_date").alias("dimension_full_date"),
            ).alias("d"),
            F.col("p.date_key") == F.col("d.d_date_key"),
            "left",
        )
        .filter(F.col("p.cutoff_date") != F.col("d.dimension_full_date"))
        .count()
    )
    if date_check:
        raise RuntimeError(f"Fact cutoff_date vs DIM_DATE mismatches: {date_check}")

    totals_missing = base.filter(F.col("stock_totals_fact_key").isNull()).count()
    print(f"STOCK_TOTALS_MATCHED_ROWS={mart_rows - totals_missing}", flush=True)
    print(f"STOCK_TOTALS_MISSING_ROWS={totals_missing}", flush=True)

    # Port share is calculated against the official total snapshot, where present.
    mart = (
        base
        .withColumn(
            "port_share_val_cert_pct",
            F.when(
                F.col("total_lots_with_val_cert") > 0,
                F.round(
                    F.col("lots_with_val_cert") * F.lit(100.0)
                    / F.col("total_lots_with_val_cert"),
                    3,
                ),
            ),
        )
        .withColumn(
            "port_share_non_tend_pct",
            F.when(
                F.col("total_lots_non_tend") > 0,
                F.round(
                    F.col("lots_non_tend") * F.lit(100.0)
                    / F.col("total_lots_non_tend"),
                    3,
                ),
            ),
        )
        .withColumn(
            "port_share_suspended_pct",
            F.when(
                F.col("total_lots_suspended") > 0,
                F.round(
                    F.col("lots_suspended") * F.lit(100.0)
                    / F.col("total_lots_suspended"),
                    3,
                ),
            ),
        )
    )

    # Compare with the previous available snapshot for each port/commodity/source.
    window = Window.partitionBy(
        "port_key", "commodity", "source_key"
    ).orderBy("cutoff_date")

    mart = (
        mart
        .withColumn("previous_cutoff_date", F.lag("cutoff_date").over(window))
        .withColumn("previous_lots_with_val_cert", F.lag("lots_with_val_cert").over(window))
        .withColumn("previous_lots_non_tend", F.lag("lots_non_tend").over(window))
        .withColumn("previous_lots_suspended", F.lag("lots_suspended").over(window))
        .withColumn(
            "days_since_previous_snapshot",
            F.datediff(F.col("cutoff_date"), F.col("previous_cutoff_date")),
        )
        .withColumn(
            "change_lots_with_val_cert",
            F.col("lots_with_val_cert") - F.col("previous_lots_with_val_cert"),
        )
        .withColumn(
            "change_lots_non_tend",
            F.col("lots_non_tend") - F.col("previous_lots_non_tend"),
        )
        .withColumn(
            "change_lots_suspended",
            F.col("lots_suspended") - F.col("previous_lots_suspended"),
        )
        .withColumn(
            "change_val_cert_pct",
            F.when(
                F.col("previous_lots_with_val_cert") > 0,
                F.round(
                    (F.col("lots_with_val_cert")
                     / F.col("previous_lots_with_val_cert") - 1) * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "change_non_tend_pct",
            F.when(
                F.col("previous_lots_non_tend") > 0,
                F.round(
                    (F.col("lots_non_tend")
                     / F.col("previous_lots_non_tend") - 1) * 100,
                    3,
                ),
            ),
        )
        .withColumn(
            "change_suspended_pct",
            F.when(
                F.col("previous_lots_suspended") > 0,
                F.round(
                    (F.col("lots_suspended")
                     / F.col("previous_lots_suspended") - 1) * 100,
                    3,
                ),
            ),
        )
        .drop("fact_port_id")
    )

    assert_unique(mart, port_grain, "MART_ROBUSTA_STOCK_ANALYSIS_GRAIN")

    print("STOCK_MART_ROWS:", mart.count(), flush=True)
    print("STOCK_PORT_COUNT:", mart.select("port_key").distinct().count(), flush=True)
    print("STOCK_COMMODITIES:", mart.select("commodity").distinct().count(), flush=True)
    print("STOCK_DATE_RANGE:", flush=True)
    mart.agg(
        F.min("cutoff_date").alias("first_cutoff_date"),
        F.max("cutoff_date").alias("last_cutoff_date"),
    ).show(truncate=False)

    mart.select(
        "cutoff_date", "port_id", "commodity",
        "lots_with_val_cert", "lots_non_tend", "lots_suspended",
        "total_lots_with_val_cert", "port_share_val_cert_pct",
        "change_lots_with_val_cert", "change_val_cert_pct",
        "reconciliation_status", "port_quality_status",
    ).orderBy(F.desc("cutoff_date"), "port_id").show(25, truncate=False)

    print("ROBUSTA_STOCK_MART_VALIDATION_PASSED", flush=True)
    return mart


def main():
    spark = create_spark()
    try:
        mart = build_mart(spark)

        if not WRITE:
            print("ROBUSTA_STOCK_MART_DRY_RUN_PASSED", flush=True)
            return

        if spark.catalog.tableExists(TARGET):
            raise RuntimeError(
                f"Target already exists; refusing to overwrite: {TARGET}"
            )

        mart.writeTo(TARGET).using("iceberg").create()
        actual = spark.table(TARGET).count()
        expected = mart.count()

        print(f"ROBUSTA_STOCK_ICEBERG_ROWS: {actual}", flush=True)
        if actual != expected:
            raise RuntimeError(
                f"Iceberg row-count mismatch: expected={expected}, actual={actual}"
            )

        print("ROBUSTA_STOCK_ICEBERG_WRITE_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
