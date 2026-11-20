
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


WRITE = "--write" in sys.argv[1:]

TARGET = "lakehouse.gold.dim_source"
SOURCE_CODE = "usda_psd"


spark = (
    SparkSession.builder
    .appName("TLCN-Migrate-DIM-Source-PSD")
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
    print("===== DIM_SOURCE PSD MIGRATION =====", flush=True)

    # 1. Check existing dimension.
    if not spark.catalog.tableExists(TARGET):
        raise RuntimeError(
            f"Target table does not exist: {TARGET}"
        )

    existing = spark.table(TARGET)

    required_columns = {
        "source_key",
        "source_code",
        "source_name"
    }

    missing = required_columns - set(existing.columns)

    if missing:
        raise RuntimeError(
            f"Missing DIM_SOURCE columns: {sorted(missing)}"
        )

    total = existing.count()

    unique_keys = (
        existing.select("source_key")
        .distinct()
        .count()
    )

    unique_codes = (
        existing.select("source_code")
        .distinct()
        .count()
    )

    null_count = existing.filter(
        F.col("source_key").isNull()
        | F.col("source_code").isNull()
        | F.col("source_name").isNull()
    ).count()

    print("DIM_SOURCE_EXISTING_ROWS:", total, flush=True)
    print("DIM_SOURCE_EXISTING_UNIQUE_KEYS:", unique_keys, flush=True)
    print("DIM_SOURCE_EXISTING_UNIQUE_CODES:", unique_codes, flush=True)
    print("DIM_SOURCE_EXISTING_NULLS:", null_count, flush=True)

    assert total == unique_keys
    assert total == unique_codes
    assert null_count == 0

    # 2. Generate the same deterministic key
    # used by gold_foundation_dimensions.py.
    candidate = (
        spark.createDataFrame(
            [(SOURCE_CODE,)],
            ["source_code"]
        )
        .select(
            F.expr(
                "CAST(CONV(SUBSTR(SHA2(source_code, 256), "
                "1, 15), 16, 10) AS BIGINT)"
            ).alias("source_key"),
            F.col("source_code"),
            F.col("source_code").alias("source_name")
        )
    )

    candidate_row = candidate.first()

    candidate_key = candidate_row["source_key"]

    print("DIM_SOURCE_PSD_CANDIDATE:", flush=True)
    candidate.show(truncate=False)

    # 3. Check natural key.
    existing_code = existing.filter(
        F.col("source_code") == SOURCE_CODE
    )

    existing_code_count = existing_code.count()

    if existing_code_count > 1:
        raise RuntimeError(
            "Duplicate source_code in DIM_SOURCE"
        )

    if existing_code_count == 1:
        old = existing_code.first()

        if old["source_key"] != candidate_key:
            raise RuntimeError(
                "Existing source_key does not match "
                "the deterministic SHA256 key"
            )

        if old["source_name"] != SOURCE_CODE:
            raise RuntimeError(
                "Existing source_name does not match "
                "the DIM_SOURCE naming convention"
            )

    # 4. Check deterministic key collision.
    key_collision = existing.filter(
        (F.col("source_key") == candidate_key)
        & (F.col("source_code") != SOURCE_CODE)
    ).count()

    if key_collision > 0:
        raise RuntimeError(
            "SHA256 source_key collision detected"
        )

    already_exists = existing_code_count == 1

    expected_rows = total + (0 if already_exists else 1)

    print(
        "DIM_SOURCE_PSD_ALREADY_EXISTS:",
        already_exists,
        flush=True
    )

    print(
        "DIM_SOURCE_PSD_EXPECTED_ROWS:",
        expected_rows,
        flush=True
    )

    # 5. Dry Run / Iceberg MERGE.
    if WRITE:

        candidate.createOrReplaceTempView(
            "stage_dim_source_psd"
        )

        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING stage_dim_source_psd s
            ON t.source_key = s.source_key
            WHEN NOT MATCHED THEN
                INSERT *
        """)

        result = spark.table(TARGET)

        actual_rows = result.count()

        actual_unique_keys = (
            result.select("source_key")
            .distinct()
            .count()
        )

        actual_unique_codes = (
            result.select("source_code")
            .distinct()
            .count()
        )

        source_result = result.filter(
            F.col("source_code") == SOURCE_CODE
        )

        source_count = source_result.count()

        assert actual_rows == expected_rows
        assert actual_unique_keys == actual_rows
        assert actual_unique_codes == actual_rows
        assert source_count == 1

        inserted = source_result.first()

        assert inserted["source_key"] == candidate_key
        assert inserted["source_name"] == SOURCE_CODE

        print(
            "DIM_SOURCE_PSD_ICEBERG_ROWS:",
            actual_rows,
            flush=True
        )

        print(
            "DIM_SOURCE_PSD_UNIQUE_KEYS:",
            actual_unique_keys,
            flush=True
        )

        print(
            "DIM_SOURCE_PSD_UNIQUE_CODES:",
            actual_unique_codes,
            flush=True
        )

        print("DIM_SOURCE_PSD_MIGRATION_PASSED", flush=True)

    else:

        print(
            "DIM_SOURCE_PSD_PLANNED_INSERTS:",
            0 if already_exists else 1,
            flush=True
        )

        print("DIM_SOURCE_PSD_DRY_RUN_PASSED", flush=True)
        print("NO_GOLD_TABLE_WRITTEN", flush=True)

finally:
    spark.stop()
