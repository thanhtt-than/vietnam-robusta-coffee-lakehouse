from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Recovered-Coffee-Production")
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
    count = spark.table(
        "lakehouse.silver.coffee_production_annual"
    ).count()

    print("RECOVERED_PRODUCTION_ROWS:", count, flush=True)

    if count != 19:
        raise RuntimeError(f"Expected 19 rows, found {count}")

    print("RECOVERED_PRODUCTION_VERIFIED", flush=True)

finally:
    spark.stop()
