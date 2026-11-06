
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TLCN-Iceberg-Smoke-Test")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config(
        "spark.sql.catalog.lakehouse.uri",
        "http://nessie:19120/iceberg/main/"
    )
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)

try:
    print("STEP 1: Create test namespace")
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.tlcn_smoke_test"
    )

    print("STEP 2: Create Iceberg table")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS
        lakehouse.tlcn_smoke_test.connection_test (
            id BIGINT,
            message STRING
        )
        USING iceberg
    """)

    print("STEP 3: Insert data")
    spark.sql("""
        INSERT INTO lakehouse.tlcn_smoke_test.connection_test
        VALUES (1, 'Spark Nessie MinIO OK')
    """)

    print("STEP 4: Read data")
    spark.sql("""
        SELECT * FROM lakehouse.tlcn_smoke_test.connection_test
    """).show(truncate=False)

    print("STEP 5: Check Iceberg snapshots")
    spark.sql("""
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.tlcn_smoke_test.connection_test.snapshots
    """).show(truncate=False)

    print("ICEBERG_END_TO_END_TEST_PASSED")

finally:
    spark.stop()
