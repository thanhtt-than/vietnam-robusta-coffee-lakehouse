from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TLCN-Diagnose-Nessie-Catalog")
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

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n=== CATALOGS ===", flush=True)
    spark.sql("SHOW CATALOGS").show(truncate=False)

    print("\n=== NAMESPACES ===", flush=True)
    spark.sql(
        "SHOW NAMESPACES IN lakehouse"
    ).show(truncate=False)

    print("\n=== SILVER TABLES ===", flush=True)
    spark.sql(
        "SHOW TABLES IN lakehouse.silver"
    ).show(100, truncate=False)

    print("\nCATALOG_DIAGNOSIS_PASSED", flush=True)

finally:
    spark.stop()
