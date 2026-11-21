"""Read-only Spark + Iceberg/Nessie health check for Airflow.

This script does not create namespaces, tables, snapshots, or rows.
It verifies that Spark starts, the Iceberg catalog can be reached, and
the expected lakehouse namespaces can be listed.
"""

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("TLCN-Airflow-Spark-Iceberg-Healthcheck")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config(
        "spark.sql.catalog.lakehouse.uri",
        "http://nessie:19120/iceberg/main/",
    )
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)

try:
    print("AIRFLOW_SPARK_HEALTHCHECK_STEP_1: SPARK_SESSION_STARTED", flush=True)

    spark.sql("SELECT 1 AS spark_connection_ok").show()

    print("AIRFLOW_SPARK_HEALTHCHECK_STEP_2: ICEBERG_CATALOG_NAMESPACES", flush=True)
    spark.sql("SHOW NAMESPACES IN lakehouse").show(truncate=False)

    print("AIRFLOW_SPARK_HEALTHCHECK_STEP_3: GOLD_NAMESPACE_TABLES", flush=True)
    spark.sql("SHOW TABLES IN lakehouse.gold").show(truncate=False)

    print("AIRFLOW_SPARK_ICEBERG_HEALTHCHECK_PASSED", flush=True)
finally:
    spark.stop()
