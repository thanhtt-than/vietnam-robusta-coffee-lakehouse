from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Recovered-VICOFA")
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
        "lakehouse.silver.vicofa_articles"
    ).count()

    print("RECOVERED_VICOFA_ROWS:", count, flush=True)

    if count != 1218:
        raise RuntimeError(f"Expected 1218 rows, found {count}")

    print("RECOVERED_VICOFA_VERIFIED", flush=True)

finally:
    spark.stop()
