from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("TLCN-Inspect-VICOFA-Bronze")
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
    df = spark.table(
        "lakehouse.bronze.vicofa_articles_raw"
    )

    print("===== VICOFA BRONZE SCHEMA =====")
    df.printSchema()

    print("VICOFA_BRONZE_TOTAL:", df.count())

    print("===== SOURCE AND RECORD TYPES =====")
    for group_col in ["source", "record_type"]:
        if group_col in df.columns:
            df.groupBy(group_col).count().show(
                30, truncate=False
            )

    print("===== VICOFA JSON FIELDS =====")

    parsed = df.withColumn(
        "json_map",
        F.from_json(
            F.col("raw_record_json"),
            "MAP<STRING,STRING>"
        )
    )

    (
        parsed
        .select(
            F.explode(
                F.map_keys("json_map")
            ).alias("field")
        )
        .distinct()
        .orderBy("field")
        .show(100, truncate=False)
    )

    print("===== JSON FIELD COVERAGE =====")

    (
        parsed
        .select(
            F.explode(
                F.map_entries("json_map")
            ).alias("entry")
        )
        .groupBy(
            F.col("entry.key").alias("field")
        )
        .agg(
            F.count("*").alias("present"),
            F.sum(
                F.when(
                    F.length(
                        F.trim(F.col("entry.value"))
                    ) > 0,
                    1
                ).otherwise(0)
            ).alias("nonempty")
        )
        .orderBy("field")
        .show(100, truncate=False)
    )

    print("===== SAMPLE BY RECORD TYPE =====")

    partition_cols = (
        ["record_type"]
        if "record_type" in df.columns
        else ["source"]
    )

    w = Window.partitionBy(
        *partition_cols
    ).orderBy(
        "object_key",
        "record_position"
    )

    samples = (
        df.withColumn(
            "rn",
            F.row_number().over(w)
        )
        .filter(F.col("rn") <= 3)
        .collect()
    )

    for row in samples:
        print("\n--- SAMPLE ---")
        for field in [
            "record_type",
            "source",
            "object_key",
            "record_position",
            "raw_record_json"
        ]:
            if field in row.asDict():
                value = row[field]
                if value is not None:
                    value = str(value)[:1500]
                print(f"{field}: {value}")

    print("VICOFA_BRONZE_INSPECTION_PASSED")

finally:
    spark.stop()
