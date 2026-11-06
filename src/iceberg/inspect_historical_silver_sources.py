from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-Inspect-Historical-Silver-Sources")
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

tables = {
    "AREA": "lakehouse.bronze.coffee_area_raw",
    "PRODUCTION": "lakehouse.bronze.coffee_production_raw",
    "TRADE": "lakehouse.bronze.coffee_trade_raw"
}

try:
    for name, table in tables.items():
        print(f"\n===== {name} =====", flush=True)

        df = spark.table(table)
        df.printSchema()

        print(f"{name}_BRONZE_ROWS:", df.count(), flush=True)

        if "source" in df.columns:
            df.groupBy("source").count().show(truncate=False)

        available = [
            col for col in [
                "record_id",
                "object_key",
                "record_position",
                "source",
                "record_type",
                "raw_record_json"
            ]
            if col in df.columns
        ]

        df.select(*available).show(
            3, truncate=250, vertical=True
        )

        if "raw_record_json" in df.columns:
            print(f"{name}_JSON_FIELDS:", flush=True)

            (
                df.select(
                    F.explode(
                        F.map_keys(
                            F.from_json(
                                "raw_record_json",
                                "MAP<STRING,STRING>"
                            )
                        )
                    ).alias("field")
                )
                .distinct()
                .orderBy("field")
                .show(100, truncate=False)
            )

    print("HISTORICAL_SOURCES_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
