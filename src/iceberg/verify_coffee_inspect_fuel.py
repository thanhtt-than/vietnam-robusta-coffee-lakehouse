from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-Silver-Coffee-Verify-Fuel-Inspect")
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

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n===== VERIFY SILVER COFFEE =====", flush=True)

    coffee = spark.table(
        "lakehouse.silver.coffee_prices_daily"
    )

    coffee.printSchema()

    print("SILVER_COFFEE_ROWS:", coffee.count(), flush=True)

    key = ["source_family", "province", "quote_date"]

    unique_keys = coffee.select(*key).distinct().count()
    print("SILVER_COFFEE_UNIQUE_KEYS:", unique_keys, flush=True)

    if "quality_status" in coffee.columns:
        coffee.groupBy("quality_status").count().show(
            truncate=False
        )

    lineage = [
        c for c in [
            "selected_bronze_record_id",
            "selected_bronze_object_key"
        ]
        if c in coffee.columns
    ]

    if lineage:
        coffee.select(*lineage).show(3, truncate=100)

    assert coffee.count() == 9409
    assert unique_keys == 9409

    print("SILVER_COFFEE_VERIFIED", flush=True)

    print("\n===== INSPECT BRONZE FUEL =====", flush=True)

    fuel = spark.table("lakehouse.bronze.fuel_prices_raw")

    fuel.printSchema()
    print("BRONZE_FUEL_ROWS:", fuel.count(), flush=True)

    if "source" in fuel.columns:
        fuel.groupBy("source").count().show(truncate=False)

    available = [
        c for c in [
            "record_id",
            "object_key",
            "record_position",
            "row_index",
            "record_type",
            "source",
            "source_layout",
            "raw_record_json"
        ]
        if c in fuel.columns
    ]

    fuel.select(*available).show(
        5, truncate=180, vertical=True
    )

    print("FUEL_BRONZE_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
