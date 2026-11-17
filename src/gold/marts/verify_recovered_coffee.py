from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Recovered-Coffee-Silver")
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
    table = "lakehouse.silver.coffee_prices_daily"
    df = spark.table(table)

    count = df.count()
    print("RECOVERED_COFFEE_ROWS:", count, flush=True)

    if count != 9409:
        raise RuntimeError(
            f"Expected 9409 rows, found {count}"
        )

    print("RECOVERED_COFFEE_VERIFIED", flush=True)

finally:
    spark.stop()
