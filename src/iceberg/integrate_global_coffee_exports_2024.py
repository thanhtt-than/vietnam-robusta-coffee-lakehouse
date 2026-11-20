"""Integrate 2024 global coffee exports from Nguoi 2 Word tables."""

import json
import re
import sys
import unicodedata
from pathlib import Path

from pyspark.sql import functions as F
from minio_direct import spark_session

WRITE = "--write" in sys.argv[1:]

BRONZE = "lakehouse.bronze.global_coffee_exports_raw"
SILVER = "lakehouse.silver.global_coffee_exports"
DIM_EXPORTER = "lakehouse.gold.dim_coffee_exporter"
DIM_SOURCE = "lakehouse.gold.dim_source"
GOLD = "lakehouse.gold.fact_global_coffee_exports"

SOURCE_CODE = "nguoi2_word_global_exports_2024"

COUNTRIES = {
    "the gioi": "WORLD",
    "brazil": "BRAZIL",
    "viet nam": "VIET_NAM",
    "thuy sy": "SWITZERLAND",
    "duc": "GERMANY",
    "colombia": "COLOMBIA",
    "y": "ITALY",
    "indonesia": "INDONESIA",
    "uganda": "UGANDA",
    "ethiopia": "ETHIOPIA",
    "phap": "FRANCE",
}


def normalize(value):
    value = unicodedata.normalize("NFD", value.casefold())
    value = "".join(
        c for c in value if unicodedata.category(c) != "Mn"
    )
    return " ".join(value.replace("\u0111", "d").split())


def number(value):
    cleaned = re.sub(r"\s+", "", value)
    cleaned = cleaned.replace(".", "").replace(",", ".")
    cleaned = cleaned.replace("%", "")
    return cleaned


def hash_key(column):
    return F.expr(
        "CAST(CONV(SUBSTR(SHA2("
        + column +
        ",256),1,15),16,10) AS BIGINT)"
    )


def merge(spark, target, df, key, ddl):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} "
        f"({ddl}) USING iceberg"
    )

    stored = spark.table(target)

    if stored.columns != df.columns:
        raise RuntimeError("Schema mismatch: " + target)

    if df.count() != df.select(key).distinct().count():
        raise RuntimeError("Duplicate incoming key: " + target)

    if stored.count() != stored.select(key).distinct().count():
        raise RuntimeError("Duplicate stored key: " + target)

    common = df.alias("a").join(
        stored.alias("b"), key, "inner"
    )

    mismatch = None

    for column in df.columns:
        if column == key:
            continue
        term = ~F.col("a." + column).eqNullSafe(
            F.col("b." + column)
        )
        mismatch = term if mismatch is None else mismatch | term

    if common.filter(mismatch).limit(1).count():
        raise RuntimeError("Existing data mismatch: " + target)

    stage = "stage_" + target.split(".")[-1]
    df.createOrReplaceTempView(stage)

    columns = ", ".join(df.columns)
    values = ", ".join("s." + c for c in df.columns)

    spark.sql(f"""
        MERGE INTO {target} t
        USING {stage} s
        ON t.{key} = s.{key}
        WHEN NOT MATCHED THEN INSERT ({columns})
        VALUES ({values})
    """)

    final = spark.table(target)

    if final.count() != df.count():
        raise RuntimeError("Unexpected row count: " + target)

    print(target, "ROWS:", final.count(), flush=True)


def main():
    candidates = [
        Path("/workspace/src/iceberg/nguoi2_word_tables.json"),
        Path("src/iceberg/nguoi2_word_tables.json"),
    ]

    path = next((p for p in candidates if p.is_file()), None)

    if path is None:
        raise RuntimeError("Word JSON not found in Spark workspace")

    documents = json.loads(path.read_text(encoding="utf-8"))

    matches = [
        d for d in documents
        if "top 10 nuoc xuat khau" in normalize(d["filename"])
    ]

    if len(matches) != 1:
        raise RuntimeError("Expected one global exports document")

    document = matches[0]
    tables = document["tables"]

    if len(tables) != 1 or len(tables[0]) != 12:
        raise RuntimeError("Unexpected source table structure")

    rows = []

    for row_number, cells in enumerate(tables[0][1:], 2):
        if len(cells) != 5:
            raise RuntimeError("Unexpected number of columns")

        name = cells[0]
        code = COUNTRIES.get(normalize(name))

        if code is None:
            raise RuntimeError("Unknown exporter: " + name)

        rows.append((
            document["filename"],
            row_number,
            code,
            name,
            json.dumps(cells, ensure_ascii=False),
            number(cells[1]),
            number(cells[2]),
            number(cells[3]),
            number(cells[4]),
        ))

    if len(rows) != 11 or len(set(r[2] for r in rows)) != 11:
        raise RuntimeError("Invalid exporter count")

    spark = spark_session("TLCN-Global-Coffee-Exports-2024")

    try:
        raw = spark.createDataFrame(
            rows,
            [
                "document_name",
                "row_number",
                "exporter_code",
                "exporter_name",
                "raw_cells_json",
                "raw_quantity",
                "raw_quantity_share",
                "raw_value",
                "raw_value_share",
            ],
        )

        bronze = raw.select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("document_name"),
                    F.col("row_number").cast("string"),
                    F.col("raw_cells_json"),
                ),
                256,
            ).alias("record_id"),
            "document_name",
            "row_number",
            "raw_cells_json",
        )

        silver = raw.join(
            bronze,
            ["document_name", "row_number", "raw_cells_json"],
        ).select(
            F.sha2(
                F.concat_ws(
                    "||", F.lit("2024"), F.col("exporter_code")
                ),
                256,
            ).alias("observation_id"),
            F.lit(2024).cast("int").alias("year"),
            "exporter_code",
            "exporter_name",
            F.col("raw_quantity").cast(
                "decimal(20,3)"
            ).alias("quantity_tonnes"),
            F.col("raw_quantity_share").cast(
                "decimal(8,3)"
            ).alias("quantity_share_pct"),
            F.col("raw_value").cast(
                "decimal(20,3)"
            ).alias("value_usd_thousand"),
            F.col("raw_value_share").cast(
                "decimal(8,3)"
            ).alias("value_share_pct"),
            F.col("record_id").alias("bronze_record_id"),
        )

        if silver.count() != 11:
            raise RuntimeError("Silver count mismatch")

        if silver.filter(
            F.col("quantity_tonnes").isNull()
            | F.col("value_usd_thousand").isNull()
            | F.col("quantity_share_pct").isNull()
            | F.col("value_share_pct").isNull()
        ).count():
            raise RuntimeError("Invalid numeric values")

        exporter = silver.select(
            "exporter_code", "exporter_name"
        ).distinct().withColumn(
            "exporter_key", hash_key("exporter_code")
        ).select(
            "exporter_key", "exporter_code", "exporter_name"
        )

        source = spark.createDataFrame(
            [
                (
                    SOURCE_CODE,
                    "Nguoi 2 Word - Global coffee exports 2024",
                )
            ],
            ["source_code", "source_name"],
        ).withColumn(
            "source_key", hash_key("source_code")
        ).select(
            "source_key", "source_code", "source_name"
        )

        years = spark.table(
            "lakehouse.gold.dim_year"
        ).filter(
            F.col("calendar_year") == 2024
        ).select("year_key")

        if years.count() != 1:
            raise RuntimeError("Missing year 2024")

        existing_sources = spark.table(DIM_SOURCE)

        if existing_sources.count() != existing_sources.select(
            "source_key"
        ).distinct().count():
            raise RuntimeError("Existing source dimension not unique")

        collisions = source.alias("a").join(
            existing_sources.alias("b"), "source_key", "inner"
        ).filter(
            F.col("a.source_code") != F.col("b.source_code")
        ).count()

        if collisions:
            raise RuntimeError("Source key collision")

        print("GLOBAL_EXPORTS_BRONZE_ROWS:", bronze.count())
        print("GLOBAL_EXPORTS_SILVER_ROWS:", silver.count())
        print("GLOBAL_EXPORTER_DIM_ROWS:", exporter.count())

        if not WRITE:
            print("GLOBAL_COFFEE_EXPORTS_DRY_RUN_PASSED")
            return

        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze"
        )
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver"
        )
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        merge(
            spark, BRONZE, bronze, "record_id",
            """
            record_id STRING,
            document_name STRING,
            row_number BIGINT,
            raw_cells_json STRING
            """
        )

        merge(
            spark, SILVER, silver, "observation_id",
            """
            observation_id STRING,
            year INT,
            exporter_code STRING,
            exporter_name STRING,
            quantity_tonnes DECIMAL(20,3),
            quantity_share_pct DECIMAL(8,3),
            value_usd_thousand DECIMAL(20,3),
            value_share_pct DECIMAL(8,3),
            bronze_record_id STRING
            """
        )

        merge(
            spark, DIM_EXPORTER, exporter, "exporter_key",
            """
            exporter_key BIGINT,
            exporter_code STRING,
            exporter_name STRING
            """
        )

        # Source is a shared dimension: insert only the new source.
        source.createOrReplaceTempView("global_export_source_stage")

        spark.sql(f"""
            MERGE INTO {DIM_SOURCE} t
            USING global_export_source_stage s
            ON t.source_code = s.source_code
            WHEN NOT MATCHED THEN
              INSERT (source_key, source_code, source_name)
              VALUES (s.source_key, s.source_code, s.source_name)
        """)

        source_in_gold = spark.table(DIM_SOURCE).filter(
            F.col("source_code") == SOURCE_CODE
        )

        if source_in_gold.count() != 1:
            raise RuntimeError("Missing Word source")

        fact = (
            silver
            .join(
                exporter.select("exporter_key", "exporter_code"),
                "exporter_code",
            )
            .crossJoin(years)
            .crossJoin(source_in_gold.select("source_key"))
            .select(
                F.col("observation_id").alias("export_fact_id"),
                "year_key",
                "exporter_key",
                "source_key",
                "quantity_tonnes",
                "quantity_share_pct",
                "value_usd_thousand",
                "value_share_pct",
                "bronze_record_id",
            )
        )

        if fact.count() != 11:
            raise RuntimeError("Gold candidate count mismatch")

        merge(
            spark, GOLD, fact, "export_fact_id",
            """
            export_fact_id STRING,
            year_key INT,
            exporter_key BIGINT,
            source_key BIGINT,
            quantity_tonnes DECIMAL(20,3),
            quantity_share_pct DECIMAL(8,3),
            value_usd_thousand DECIMAL(20,3),
            value_share_pct DECIMAL(8,3),
            bronze_record_id STRING
            """
        )

        print("GLOBAL_COFFEE_EXPORTS_PIPELINE_PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
