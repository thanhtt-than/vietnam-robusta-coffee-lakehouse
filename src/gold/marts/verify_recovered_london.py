from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Recovered-London")
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.lakehouse.type",
        "rest"
    )
    .config(
        "spark.sql.catalog.lakehouse.uri",
        "http://nessie:19120/iceberg/main/"
    )
    .getOrCreate()
)

try:
    count = spark.table(
        "lakehouse.silver.london_futures_daily"
    ).count()

    print("RECOVERED_LONDON_ROWS:", count, flush=True)

    if count != 4755:
        raise RuntimeError(
            f"Expected 4755 rows, found {count}"
        )

    print("RECOVERED_LONDON_VERIFIED", flush=True)

finally:
    spark.stop()
