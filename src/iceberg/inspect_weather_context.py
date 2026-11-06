from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("TLCN-Weather-Station-Context")
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
    weather = spark.table("lakehouse.bronze.weather_raw")

    print("===== SOURCE CONTEXT COVERAGE =====")

    weather.groupBy("source", "record_type").agg(
        F.count("*").alias("records"),
        F.countDistinct("source_context_json").alias("contexts"),
        F.sum(
            F.when(F.col("source_context_json").isNull(), 1)
             .otherwise(0)
        ).alias("null_contexts")
    ).show(30, truncate=False)

    print("===== SAMPLE CONTEXT BY SOURCE =====")

    window = Window.partitionBy(
        "source", "record_type"
    ).orderBy("object_key", "record_position")

    samples = (
        weather
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select(
            "source",
            "record_type",
            "object_key",
            "source_context_json",
            "raw_record_json"
        )
        .collect()
    )

    for row in samples:
        print("\nSOURCE:", row["source"])
        print("TYPE:", row["record_type"])
        print("OBJECT:", row["object_key"])
        print("CONTEXT:", row["source_context_json"])
        print("RAW_SAMPLE:", row["raw_record_json"][:700])

    print("WEATHER_CONTEXT_INSPECTION_PASSED")

finally:
    spark.stop()
