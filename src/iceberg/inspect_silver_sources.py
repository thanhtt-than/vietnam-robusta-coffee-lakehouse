from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-Silver-Schema-Inspection")
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
    tables = [
        "lakehouse.bronze.coffee_prices_raw",
        "lakehouse.bronze.domestic_prices_raw"
    ]

    for table in tables:
        print("\n=== TABLE:", table, "===", flush=True)

        df = spark.table(table)
        df.printSchema()

        print("ROW_COUNT:", df.count(), flush=True)

        available_columns = [
            column for column in [
                "record_id",
                "object_key",
                "row_index",
                "source",
                "record_type",
                "quote_date_vn",
                "product",
                "raw_record_json"
            ]
            if column in df.columns
        ]

        print("AVAILABLE_COLUMNS:", available_columns, flush=True)

        print("RAW_JSON_SAMPLES:", flush=True)

        df.select(*available_columns).show(
            3,
            truncate=180,
            vertical=True
        )

        if "raw_record_json" in df.columns:
            print("RAW_JSON_FIELD_NAMES:", flush=True)

            df.select(
                F.explode(
                    F.map_keys(
                        F.from_json(
                            F.col("raw_record_json"),
                            "MAP<STRING,STRING>"
                        )
                    )
                ).alias("json_field")
            ).distinct().orderBy("json_field").show(
                100,
                truncate=False
            )

    print("SILVER_SCHEMA_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
