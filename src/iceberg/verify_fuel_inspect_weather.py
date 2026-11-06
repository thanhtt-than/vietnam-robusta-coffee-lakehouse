from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-Verify-Fuel-Inspect-Weather")
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
    print("===== VERIFY SILVER FUEL =====", flush=True)

    fuel = spark.table(
        "lakehouse.silver.fuel_price_observations"
    )

    fuel.printSchema()

    total = fuel.count()
    print("SILVER_FUEL_ROWS:", total, flush=True)

    print("QUALITY_STATUS:", flush=True)
    fuel.groupBy("quality_status").count().show(
        truncate=False
    )

    key = [
        "source_family",
        "series_code",
        "region",
        "quote_date",
        "unit"
    ]

    if all(col in fuel.columns for col in key):
        unique = fuel.select(*key).distinct().count()
        print("SILVER_FUEL_UNIQUE_KEYS:", unique, flush=True)
        assert unique == total

    assert total == 1704
    print("SILVER_FUEL_VERIFIED", flush=True)

    print("\n===== INSPECT BRONZE WEATHER =====", flush=True)

    weather = spark.table("lakehouse.bronze.weather_raw")

    weather.printSchema()
    print("BRONZE_WEATHER_ROWS:", weather.count(), flush=True)

    for column in ["source", "record_type", "domain"]:
        if column in weather.columns:
            print("GROUP_BY:", column, flush=True)
            weather.groupBy(column).count().show(
                30, truncate=False
            )

    available = [
        col for col in [
            "record_id",
            "object_key",
            "record_position",
            "row_index",
            "source",
            "record_type",
            "source_layout",
            "raw_record_json"
        ]
        if col in weather.columns
    ]

    weather.select(*available).show(
        5, truncate=200, vertical=True
    )

    print("WEATHER_JSON_FIELDS:", flush=True)

    weather.select(
        F.explode(
            F.map_keys(
                F.from_json(
                    F.col("raw_record_json"),
                    "MAP<STRING,STRING>"
                )
            )
        ).alias("json_field")
    ).distinct().orderBy("json_field").show(
        100, truncate=False
    )

    print("WEATHER_BRONZE_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
