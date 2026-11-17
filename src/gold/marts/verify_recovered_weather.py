from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Recovered-Weather")
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
        "lakehouse.silver.weather_daily"
    ).count()

    print("RECOVERED_WEATHER_ROWS:", count, flush=True)

    if count != 97620:
        raise RuntimeError(
            f"Expected 97620 rows, found {count}"
        )

    print("RECOVERED_WEATHER_VERIFIED", flush=True)

finally:
    spark.stop()
