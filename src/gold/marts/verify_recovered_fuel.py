from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Recovered-Fuel")
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
        "lakehouse.silver.fuel_price_observations"
    ).count()

    print("RECOVERED_FUEL_ROWS:", count, flush=True)

    if count != 1704:
        raise RuntimeError(
            f"Expected 1704 rows, found {count}"
        )

    print("RECOVERED_FUEL_VERIFIED", flush=True)

finally:
    spark.stop()
