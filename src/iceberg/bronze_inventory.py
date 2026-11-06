
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

CATALOG = "lakehouse"
TABLE = "lakehouse.bronze.raw_object_inventory"
PROFILE = "/workspace/output/bronze_audit/content_profile/object_profile.csv"

spark = (
    SparkSession.builder
    .appName("TLCN-Bronze-Raw-Inventory")
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
    .getOrCreate()
)

try:
    # Doc bao cao duoc tao tu 311 object MinIO.
    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .csv(PROFILE)
        .select(
            col("object_key"),
            col("domain"),
            col("source"),
            col("format"),
            col("structure"),
            col("status"),
            col("size_bytes").cast("long").alias("size_bytes"),
            col("row_count").cast("long").alias("row_count"),
            col("sha256")
        )
    )

    total = df.count()
    unique_keys = df.select("object_key").distinct().count()

    print(f"PROFILE_OBJECTS: {total}")
    print(f"UNIQUE_OBJECT_KEYS: {unique_keys}")

    if total != unique_keys:
        raise RuntimeError("Duplicate object_key in profile")

    if df.filter(col("object_key").isNull()).count():
        raise RuntimeError("Null object_key detected")

    if df.filter(col("status") != "OK").count():
        raise RuntimeError("Profile contains non-OK objects")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS
        lakehouse.bronze.raw_object_inventory (
            object_key STRING,
            domain STRING,
            source STRING,
            format STRING,
            structure STRING,
            status STRING,
            size_bytes BIGINT,
            row_count BIGINT,
            sha256 STRING
        )
        USING iceberg
    """)

    # Chi nap object_key chua ton tai.
    existing = spark.table(TABLE).select("object_key")

    new_objects = df.join(
        existing,
        on="object_key",
        how="left_anti"
    )

    new_count = new_objects.count()
    print(f"NEW_OBJECTS: {new_count}")

    if new_count:
        new_objects.select(
            "object_key", "domain", "source",
            "format", "structure", "status",
            "size_bytes", "row_count", "sha256"
        ).writeTo(TABLE).append()

    print("BRONZE_INVENTORY_TOTAL:", spark.table(TABLE).count())

    spark.sql("""
        SELECT domain, count(*) AS objects
        FROM lakehouse.bronze.raw_object_inventory
        GROUP BY domain
        ORDER BY domain
    """).show(30, truncate=False)

    print("BRONZE_INVENTORY_PASSED")

finally:
    spark.stop()
