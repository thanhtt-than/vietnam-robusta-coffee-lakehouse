"""Read-only diagnosis of monthly coffee trade Fact-to-Dimension joins."""
from pyspark.sql import SparkSession, functions as F

FACT = "lakehouse.gold.fact_coffee_trade_monthly"
DIM_DATE = "lakehouse.gold.dim_date"
DIM_DESTINATION = "lakehouse.gold.dim_destination"
DIM_GEOGRAPHY = "lakehouse.gold.dim_geography"
DIM_SOURCE = "lakehouse.gold.dim_source"


def create_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Diagnose-Trade-Monthly-Dimension-Joins")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri",
                "http://nessie:19120/iceberg/main/")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def inspect_fk(fact, fk, dim, dk, label, extra_columns=None):
    print(f"\\n===== {label} =====", flush=True)
    print("FACT_FK_NULL_ROWS:",
          fact.filter(F.col(fk).isNull()).count(), flush=True)

    fact_keys = fact.select(F.col(fk)).distinct()
    dim_keys = dim.select(F.col(dk)).distinct()
    unmatched = fact_keys.join(
        dim_keys, fact_keys[fk] == dim_keys[dk], "left_anti"
    )

    print("FACT_DISTINCT_KEYS:", fact_keys.count(), flush=True)
    print("DIM_DISTINCT_KEYS:", dim_keys.count(), flush=True)
    print("UNMATCHED_DISTINCT_FK:", unmatched.count(), flush=True)
    unmatched.show(30, truncate=False)

    joined = fact.alias("f").join(
        dim.alias("d"), F.col(f"f.{fk}") == F.col(f"d.{dk}"), "left"
    )
    print("UNMATCHED_FACT_ROWS:",
          joined.filter(F.col(f"d.{dk}").isNull()).count(), flush=True)

    for col in (extra_columns or []):
        missing = joined.filter(F.col(f"d.{col}").isNull()).count()
        print(f"DIM_{col.upper()}_NULL_ROWS:", missing, flush=True)
        (
            joined.filter(F.col(f"d.{col}").isNull())
            .select(F.col(f"f.{fk}").alias(fk),
                    F.col("f.market_code"))
            .distinct()
            .show(20, truncate=False)
        )


def main():
    spark = create_spark()
    try:
        fact = spark.table(FACT)
        dates = spark.table(DIM_DATE)
        destinations = spark.table(DIM_DESTINATION)
        geography = spark.table(DIM_GEOGRAPHY)
        sources = spark.table(DIM_SOURCE)

        print("FACT_SCHEMA:", flush=True)
        fact.printSchema()
        for label, df in [
            ("DIM_DATE", dates), ("DIM_DESTINATION", destinations),
            ("DIM_GEOGRAPHY", geography), ("DIM_SOURCE", sources)
        ]:
            print(f"{label}_COLUMNS:", df.columns, flush=True)

        print("\\n===== FACT KEY NULL COUNTS =====", flush=True)
        for key in ["date_key", "destination_key", "source_key",
                    "geography_key"]:
            print(key, fact.filter(F.col(key).isNull()).count(), flush=True)

        inspect_fk(fact, "date_key", dates, "date_key", "DATE",
                   ["full_date"])
        inspect_fk(fact, "destination_key", destinations,
                   "destination_key", "DESTINATION",
                   ["partner_code", "partner_name"])
        inspect_fk(fact, "geography_key", geography,
                   "geography_key", "GEOGRAPHY",
                   ["geography_code", "geography_name"])
        inspect_fk(fact, "source_key", sources,
                   "source_key", "SOURCE",
                   ["source_code", "source_name"])

        print("\\n===== FACT DATE KEY / CALENDAR CONSISTENCY =====", flush=True)
        date_join = fact.alias("f").join(
            dates.alias("d"),
            F.col("f.date_key") == F.col("d.date_key"), "left"
        )
        date_join.select(
            F.col("f.date_key"),
            F.col("f.year").alias("fact_year"),
            F.col("f.month").alias("fact_month"),
            F.col("d.full_date"),
            F.col("d.year").alias("dim_year"),
            F.col("d.month").alias("dim_month"),
        ).orderBy("fact_year", "fact_month").show(30, truncate=False)

        print("\\n===== FACT FOREIGN KEY SAMPLES =====", flush=True)
        fact.select(
            "date_key", "destination_key", "source_key", "geography_key",
            "year", "month", "market_code", "market_type"
        ).orderBy("year", "month", "market_code").show(30, truncate=False)

        print("\\nTRADE_MONTHLY_JOIN_DIAGNOSTIC_COMPLETED", flush=True)
        print("READ_ONLY_NO_TABLES_WRITTEN", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
