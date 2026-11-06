from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-Verify-Weather-Inspect-London")
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
    print("===== VERIFY SILVER WEATHER =====", flush=True)

    weather = spark.table("lakehouse.silver.weather_daily")
    weather.printSchema()

    total = weather.count()
    print("SILVER_WEATHER_ROWS:", total, flush=True)

    key = ["source_series_id", "observation_date"]

    unique = weather.select(*key).distinct().count()
    print("SILVER_WEATHER_UNIQUE_KEYS:", unique, flush=True)

    print("WEATHER_QUALITY_STATUS:", flush=True)
    weather.groupBy("quality_status").count().show(
        truncate=False
    )

    assert total == 97620
    assert unique == total

    print("SILVER_WEATHER_VERIFIED", flush=True)

    print("\n===== INSPECT BRONZE LONDON FUTURES =====", flush=True)

    london = spark.table(
        "lakehouse.bronze.london_futures_raw"
    )

    london.printSchema()

    print("BRONZE_LONDON_ROWS:", london.count(), flush=True)

    available = [
        c for c in [
            "record_id",
            "object_key",
            "record_position",
            "row_index",
            "source",
            "record_type",
            "raw_record_json"
        ]
        if c in london.columns
    ]

    london.select(*available).show(
        8, truncate=250, vertical=True
    )

    print("LONDON_JSON_FIELDS:", flush=True)

    london.select(
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

    print("LONDON_BRONZE_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
