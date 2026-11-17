from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Coffee-Source-Selection")
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

try:
    df = spark.table("lakehouse.silver.coffee_prices_daily")

    priority = (
        F.when(F.col("source_family") == "bbw_coffee", 1)
        .when(F.col("source_family") == "legacy_api_json", 2)
        .when(F.col("source_family") == "enriched_csv", 3)
        .otherwise(99)
    )

    window = Window.partitionBy(
        "quote_date", "province"
    ).orderBy(
        F.col("source_priority"),
        F.col("selected_bronze_record_id")
    )

    selected = (
        df.withColumn("source_priority", priority)
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
    )

    print("SELECTED_TOTAL:", selected.count(), flush=True)

    print("SELECTED_BY_SOURCE:", flush=True)
    selected.groupBy("source_family").count().orderBy(
        "source_family"
    ).show(truncate=False)

    print("COFFEE_SELECTION_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
