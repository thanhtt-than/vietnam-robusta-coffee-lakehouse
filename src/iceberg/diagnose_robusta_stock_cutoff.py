"""
Read-only diagnostic for the Robusta Stock Silver date parsing failure.

It inspects the one Bronze source object whose parsed CutOffDate count is zero.
It does not create or modify any Iceberg table.
"""
from pyspark.sql import SparkSession, functions as F

BRONZE = "lakehouse.bronze.robusta_stock_raw"
BAD_OBJECT_SUFFIX = "Stock_Report_RC_20260514_103145.csv"


def make_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Diagnose-Robusta-Stock-CutoffDate")
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


def main():
    spark = make_spark()
    try:
        bronze = spark.table(BRONZE)

        target = bronze.filter(
            F.col("source_file") == BAD_OBJECT_SUFFIX
        )

        count = target.count()
        print("TARGET_SOURCE_FILE:", BAD_OBJECT_SUFFIX, flush=True)
        print("TARGET_BRONZE_ROWS:", count, flush=True)

        if count == 0:
            print("SOURCE_FILE_NOT_FOUND; matching object keys:", flush=True)
            bronze.filter(
                F.col("object_key").contains(BAD_OBJECT_SUFFIX)
            ).select("object_key", "source_file").distinct().show(
                20, truncate=False
            )
            return

        print("===== OBJECT METADATA =====", flush=True)
        target.select(
            "object_key", "batch_id", "ingestion_date",
            "source_file", "record_type"
        ).distinct().show(20, truncate=False)

        parsed = target.withColumn(
            "_payload",
            F.from_json("raw_record_json", "MAP<STRING,STRING>")
        ).withColumn(
            "_raw_cutoff_date",
            F.element_at(F.col("_payload"), F.lit("CutOffDate"))
        )

        print("===== RECORD TYPE COUNTS =====", flush=True)
        parsed.groupBy("record_type").count().show(truncate=False)

        print("===== DISTINCT RAW CUTOFFDATE VALUES =====", flush=True)
        (
            parsed.filter(F.col("record_type") == "port_stock")
            .groupBy("_raw_cutoff_date")
            .count()
            .orderBy(F.col("count").desc())
            .show(100, truncate=False)
        )

        print("===== PORT STOCK SAMPLE =====", flush=True)
        (
            parsed.filter(F.col("record_type") == "port_stock")
            .select(
                "record_position",
                F.element_at(F.col("_payload"), F.lit("Commodity")).alias("Commodity"),
                "_raw_cutoff_date",
                F.element_at(F.col("_payload"), F.lit("PortId")).alias("PortId"),
                F.element_at(F.col("_payload"), F.lit("LotsWithValCert")).alias("LotsWithValCert"),
                "raw_record_json"
            )
            .orderBy("record_position")
            .show(30, truncate=False)
        )

        print("===== RAW JSON KEY PROFILE =====", flush=True)
        first = (
            parsed.filter(F.col("record_type") == "port_stock")
            .select("raw_record_json")
            .orderBy("record_position")
            .first()
        )
        if first:
            print(first["raw_record_json"], flush=True)

        print("STOCK_CUTOFF_DIAGNOSTIC_PASSED", flush=True)
        print("READ_ONLY: TRUE", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
