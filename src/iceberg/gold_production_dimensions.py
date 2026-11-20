"""Gold production dimension: generic coffee product member.

Read-only by default. Use --write to MERGE into lakehouse.gold.dim_coffee_product.
All current DOCX Statistics sources describe coffee without a reliable
Arabica/Robusta split, so they map to COFFEE_UNSPECIFIED.
"""
import sys
from pyspark.sql import SparkSession, functions as F

WRITE = "--write" in sys.argv[1:]
TARGET = "lakehouse.gold.dim_coffee_product"

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Production-Dimensions")
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

spark.sparkContext.setLogLevel("ERROR")

try:
    rows = [
        (
            1,
            "COFFEE_UNSPECIFIED",
            "Coffee - Variety Not Specified",
            "UNSPECIFIED",
            "UNSPECIFIED",
            False,
        )
    ]
    product = spark.createDataFrame(
        rows,
        """
        coffee_product_key INT,
        product_code STRING,
        product_name STRING,
        variety STRING,
        grade STRING,
        is_variety_specified BOOLEAN
        """
    )

    total = product.count()
    unique_key = product.select("coffee_product_key").distinct().count()
    unique_code = product.select("product_code").distinct().count()
    nulls = product.filter(
        F.col("coffee_product_key").isNull()
        | F.col("product_code").isNull()
        | F.col("product_name").isNull()
        | F.col("variety").isNull()
        | F.col("grade").isNull()
        | F.col("is_variety_specified").isNull()
    ).count()

    print("DIM_COFFEE_PRODUCT_ROWS:", total, flush=True)
    print("DIM_COFFEE_PRODUCT_UNIQUE_KEYS:", unique_key, flush=True)
    print("DIM_COFFEE_PRODUCT_UNIQUE_CODES:", unique_code, flush=True)
    print("DIM_COFFEE_PRODUCT_NULL_REQUIRED:", nulls, flush=True)
    product.show(truncate=False)

    if total != 1 or unique_key != 1 or unique_code != 1 or nulls != 0:
        raise RuntimeError("DIM_COFFEE_PRODUCT validation failed")

    print("GOLD_PRODUCTION_DIMENSIONS_DRY_RUN_PASSED", flush=True)

    if WRITE:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                coffee_product_key INT,
                product_code STRING,
                product_name STRING,
                variety STRING,
                grade STRING,
                is_variety_specified BOOLEAN
            ) USING iceberg
        """)

        target = spark.table(TARGET)
        if target.columns != product.columns:
            raise RuntimeError(
                f"Schema mismatch for {TARGET}: "
                f"target={target.columns}, stage={product.columns}"
            )

        product.createOrReplaceTempView("stage_dim_coffee_product")
        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_dim_coffee_product s
            ON t.coffee_product_key = s.coffee_product_key
            WHEN MATCHED AND (
                NOT (t.product_code <=> s.product_code)
                OR NOT (t.product_name <=> s.product_name)
                OR NOT (t.variety <=> s.variety)
                OR NOT (t.grade <=> s.grade)
                OR NOT (t.is_variety_specified <=> s.is_variety_specified)
            ) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        saved = spark.table(TARGET)
        saved_count = saved.count()
        saved_unique = saved.select("coffee_product_key").distinct().count()
        if saved_count != 1 or saved_unique != 1:
            raise RuntimeError(
                f"{TARGET} write verification failed: "
                f"rows={saved_count}, unique_keys={saved_unique}"
            )

        print("DIM_COFFEE_PRODUCT_ICEBERG_ROWS:", saved_count, flush=True)
        print("DIM_COFFEE_PRODUCT_ICEBERG_PASSED", flush=True)
    else:
        print("NO_GOLD_TABLE_WRITTEN", flush=True)
finally:
    spark.stop()
