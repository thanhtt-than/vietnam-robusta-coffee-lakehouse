"""
TLCN Silver pipeline for the newly ingested coffee sources.

Inputs:
  lakehouse.bronze.coffee_statistics_raw  (7 DOCX, 75 raw table rows)
  lakehouse.bronze.coffee_psd_raw         (2 CSV, 87,723 raw rows)

Outputs:
  lakehouse.silver.coffee_statistics_observations
  lakehouse.silver.coffee_psd_observations

Default mode is a read-only DRY RUN. Use --write only after reviewing
the data-quality and row-count output.

Run from D:\\TLCN:
  .\\run_spark.ps1 -Script "src/iceberg/silver_coffee_new_sources.py"
  .\\run_spark.ps1 -Script "src/iceberg/silver_coffee_new_sources.py" -ScriptArgs "--write"

Important:
- Bronze is never modified.
- Vietnamese DOCX numbers are parsed using dot-thousands/comma-decimal rules.
- PSD long and wide CSV rows are normalized into one observation schema.
- The Vietnam 2015-2025 DOCX has merged Word cells. The Bronze extractor
  preserves physical XML cells, so its rows contain four cells, not the five
  visual grid positions exposed by some DOCX readers. The physical columns
  are year, area, yield, and production.
- Missing source values remain NULL; they are never converted to zero.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from functools import reduce

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


STATISTICS_BRONZE = "lakehouse.bronze.coffee_statistics_raw"
PSD_BRONZE = "lakehouse.bronze.coffee_psd_raw"

STATISTICS_SILVER = "lakehouse.silver.coffee_statistics_observations"
PSD_SILVER = "lakehouse.silver.coffee_psd_observations"
SILVER_NAMESPACE = "lakehouse.silver"

EXPECTED_STATISTICS_BRONZE_ROWS = 75
EXPECTED_PSD_BRONZE_ROWS = 87723
EXPECTED_STATISTICS_OUTPUT_ROWS = 301
EXPECTED_PSD_LONG_ROWS = 87704
EXPECTED_PSD_WIDE_ROWS = 19
EXPECTED_PSD_WIDE_YEAR_COLUMNS = 67
EXPECTED_PSD_OUTPUT_ROWS = (
    EXPECTED_PSD_LONG_ROWS
    + EXPECTED_PSD_WIDE_ROWS * EXPECTED_PSD_WIDE_YEAR_COLUMNS
)

MISSING_MARKERS = {"", "-", "—", "–", "NA", "N/A", "NULL", "NAN"}


# Layout definitions are based on the seven source DOCX table titles and
# their inspected headers. Column indexes are zero-based within cells[].
STATISTICS_DOC_SPECS = {
    "Diện tích cà phê theo địa phương giai đoạn 2021-2025 (Ha).docx": {
        "table_code": "AREA_BY_PROVINCE",
        "layout": "GEOGRAPHY_BY_YEAR",
        "geography_level": "PROVINCE",
        "expected_data_rows": 13,
        "metrics": [
            {
                "column_index": None,
                "metric_code": "COFFEE_AREA",
                "metric_label": "Diện tích cà phê",
                "source_unit": "ha",
                "standardized_unit": "ha",
                "multiplier": Decimal("1"),
            }
        ],
    },
    "Diện tích gieo trồng cà phê theo vùng giai đoạn 2020-2025 (nghìn ha).docx": {
        "table_code": "PLANTED_AREA_BY_REGION",
        "layout": "GEOGRAPHY_BY_YEAR",
        "geography_level": "REGION",
        "expected_data_rows": 5,
        "metrics": [
            {
                "column_index": None,
                "metric_code": "PLANTED_AREA",
                "metric_label": "Diện tích gieo trồng cà phê",
                "source_unit": "nghìn ha",
                "standardized_unit": "ha",
                "multiplier": Decimal("1000"),
            }
        ],
    },
    "Diện tích, năng suất, sản lượng cà phê Việt Nam giai đoạn 2015-2025.docx": {
        "table_code": "VIETNAM_AREA_YIELD_PRODUCTION",
        "layout": "YEAR_BY_METRIC",
        "geography_level": "COUNTRY",
        "geography_name": "Việt Nam",
        "expected_data_rows": 11,
        "metrics": [
            {
                "column_index": 1,
                "metric_code": "COFFEE_AREA",
                "metric_label": "Diện tích",
                "source_unit": "nghìn ha",
                "standardized_unit": "ha",
                "multiplier": Decimal("1000"),
            },
            {
                "column_index": 2,
                "metric_code": "YIELD",
                "metric_label": "Năng suất",
                "source_unit": "tấn/ha",
                "standardized_unit": "tấn/ha",
                "multiplier": Decimal("1"),
            },
            {
                "column_index": 3,
                "metric_code": "PRODUCTION",
                "metric_label": "Sản lượng",
                "source_unit": "nghìn tấn",
                "standardized_unit": "tấn",
                "multiplier": Decimal("1000"),
            },
        ],
    },
    "Diện tích, năng suất, sản lượng cà phê của một số nước lớn trên thế giới 2024.docx": {
        "table_code": "WORLD_MAJOR_COUNTRIES_2024",
        "layout": "GEOGRAPHY_METRICS_SINGLE_YEAR",
        "geography_level": "COUNTRY",
        "year": 2024,
        "expected_data_rows": 16,
        "metrics": [
            {
                "column_index": 1,
                "metric_code": "HARVESTED_AREA",
                "metric_label": "Diện tích thu hoạch",
                "source_unit": "ha",
                "standardized_unit": "ha",
                "multiplier": Decimal("1"),
            },
            {
                "column_index": 2,
                "metric_code": "PRODUCTION",
                "metric_label": "Sản lượng",
                "source_unit": "tấn",
                "standardized_unit": "tấn",
                "multiplier": Decimal("1"),
            },
            {
                "column_index": 3,
                "metric_code": "YIELD",
                "metric_label": "Năng suất",
                "source_unit": "tấn/ha",
                "standardized_unit": "tấn/ha",
                "multiplier": Decimal("1"),
            },
        ],
    },
    "Năng suất cà phê cả năm theo vùng giai đoạn 2020-2025 (tấnha).docx": {
        "table_code": "YIELD_BY_REGION",
        "layout": "GEOGRAPHY_BY_YEAR",
        "geography_level": "REGION",
        "expected_data_rows": 5,
        "metrics": [
            {
                "column_index": None,
                "metric_code": "YIELD",
                "metric_label": "Năng suất cà phê",
                "source_unit": "tấn/ha",
                "standardized_unit": "tấn/ha",
                "multiplier": Decimal("1"),
            }
        ],
    },
    "Sản lượng cà phê theo địa phương giai đoạn 2021-2025 (Tấn).docx": {
        "table_code": "PRODUCTION_BY_PROVINCE",
        "layout": "GEOGRAPHY_BY_YEAR",
        "geography_level": "PROVINCE",
        "expected_data_rows": 13,
        "metrics": [
            {
                "column_index": None,
                "metric_code": "PRODUCTION",
                "metric_label": "Sản lượng cà phê",
                "source_unit": "tấn",
                "standardized_unit": "tấn",
                "multiplier": Decimal("1"),
            }
        ],
    },
    "Sản lượng cà phê theo vùng giai đoạn 2020-2025 (nghìn tấn).docx": {
        "table_code": "PRODUCTION_BY_REGION",
        "layout": "GEOGRAPHY_BY_YEAR",
        "geography_level": "REGION",
        "expected_data_rows": 5,
        "metrics": [
            {
                "column_index": None,
                "metric_code": "PRODUCTION",
                "metric_label": "Sản lượng cà phê",
                "source_unit": "nghìn tấn",
                "standardized_unit": "tấn",
                "multiplier": Decimal("1000"),
            }
        ],
    },
}


def make_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Silver-Coffee-New-Sources")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_vietnamese_decimal(raw_value: str | None) -> Decimal | None:
    """Parse DOCX numeric text: 710.591,2 -> 710591.2; 12.477.571 -> 12477571."""
    if raw_value is None:
        return None

    value = str(raw_value).strip()
    if value.upper() in MISSING_MARKERS:
        return None

    value = value.replace(" ", "")
    if "," in value:
        value = value.replace(".", "").replace(",", ".")
    elif value.count(".") > 1:
        value = value.replace(".", "")
    elif re.fullmatch(r"-?\d{1,3}(?:\.\d{3})+", value):
        value = value.replace(".", "")

    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def parse_psd_decimal_expression(raw_column):
    """USDA CSV uses comma grouping and dot decimal notation."""
    cleaned = F.regexp_replace(F.trim(raw_column), ",", "")
    numeric_pattern = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$"
    return F.when(
        cleaned.rlike(numeric_pattern),
        cleaned.cast(DecimalType(24, 6)),
    ).otherwise(F.lit(None).cast(DecimalType(24, 6)))


def geography_level_for(name: str, default_level: str) -> str:
    normalized = name.strip().casefold()
    if normalized == "cả nước":
        return "COUNTRY_TOTAL"
    if normalized in {"tổng thế giới", "toàn thế giới"}:
        return "WORLD_TOTAL"
    return default_level


def parse_year_header(value: str) -> tuple[int, bool]:
    match = re.search(r"(?:19|20)\d{2}", value)
    if not match:
        raise ValueError(f"Cannot extract year from DOCX header: {value!r}")
    return int(match.group(0)), ("sơ bộ" in value.casefold())


def build_statistics_rows(spark):
    bronze = spark.table(STATISTICS_BRONZE)

    required = {
        "record_id", "object_key", "record_position", "domain",
        "source", "record_type", "object_sha256", "raw_record_json",
    }
    missing = required - set(bronze.columns)
    if missing:
        raise RuntimeError(
            f"Coffee Statistics Bronze schema missing columns: {sorted(missing)}"
        )

    total = bronze.count()
    unique = bronze.select("record_id").distinct().count()
    if total != EXPECTED_STATISTICS_BRONZE_ROWS or unique != total:
        raise RuntimeError(
            f"Unexpected Statistics Bronze counts: rows={total}, unique={unique}"
        )

    rows = (
        bronze.select(
            "record_id", "object_key", "record_position", "object_sha256",
            "raw_record_json",
        )
        .orderBy("object_key", "record_position")
        .collect()
    )

    grouped = {}
    for row in rows:
        payload = json.loads(row["raw_record_json"])
        filename = payload["source_filename"]
        grouped.setdefault(row["object_key"], []).append({
            "record_id": row["record_id"],
            "object_key": row["object_key"],
            "record_position": int(row["record_position"]),
            "object_sha256": row["object_sha256"],
            "source_filename": filename,
            "cells": payload["extracted_record"]["cells"],
        })

    if len(grouped) != 7:
        raise RuntimeError(
            f"Expected 7 Statistics DOCX objects, found {len(grouped)}"
        )

    output = []
    per_object = {}

    for object_key, object_rows in sorted(grouped.items()):
        filename = object_rows[0]["source_filename"]
        spec = STATISTICS_DOC_SPECS.get(filename)
        if not spec:
            raise RuntimeError(f"Unrecognized Statistics DOCX: {filename}")

        object_rows.sort(key=lambda x: x["record_position"])
        header = object_rows[0]["cells"]
        data_rows = object_rows[1:]

        if len(data_rows) != spec["expected_data_rows"]:
            raise RuntimeError(
                f"{filename}: expected {spec['expected_data_rows']} data rows, "
                f"found {len(data_rows)}"
            )

        if spec["layout"] == "GEOGRAPHY_BY_YEAR":
            year_columns = []
            for column_index, label in enumerate(header[1:], start=1):
                year, preliminary = parse_year_header(label)
                year_columns.append((column_index, year, preliminary, label))
            if len(year_columns) != 5 and filename.startswith(
                "Diện tích cà phê theo địa phương"
            ):
                raise RuntimeError(
                    f"Unexpected year columns in {filename}: {year_columns}"
                )
            if not year_columns:
                raise RuntimeError(f"No year columns found in {filename}")

        for source_row in data_rows:
            cells = source_row["cells"]
            if len(cells) != len(header):
                raise RuntimeError(
                    f"{filename} row {source_row['record_position']} has "
                    f"{len(cells)} cells; header has {len(header)}"
                )

            if spec["layout"] == "GEOGRAPHY_BY_YEAR":
                geography_name = str(cells[0]).strip()
                if not geography_name:
                    raise RuntimeError(
                        f"Missing geography in {filename}, "
                        f"record_position={source_row['record_position']}"
                    )
                for metric in spec["metrics"]:
                    for column_index, year, preliminary, year_label in year_columns:
                        raw_value = str(cells[column_index]).strip()
                        parsed = parse_vietnamese_decimal(raw_value)
                        force_review = metric.get("force_review", False)
                        if force_review:
                            quality = "REVIEW_DUPLICATE_HEADER"
                        elif raw_value.upper() in MISSING_MARKERS:
                            quality = "MISSING_SOURCE_VALUE"
                        elif parsed is None:
                            quality = "INVALID_NUMERIC_VALUE"
                        else:
                            quality = "PASS"

                        standardized = (
                            parsed * metric["multiplier"]
                            if parsed is not None else None
                        )
                        key = (
                            f"{source_row['record_id']}|{year}|"
                            f"{metric['metric_code']}"
                        )
                        output.append({
                            "silver_record_id": sha256_text(key),
                            "year": year,
                            "is_preliminary": preliminary,
                            "geography_level": geography_level_for(
                                geography_name, spec["geography_level"]
                            ),
                            "geography_name": geography_name,
                            "metric_code": metric["metric_code"],
                            "metric_label": metric["metric_label"],
                            "source_column_index": column_index,
                            "raw_value": raw_value,
                            "value_source": parsed,
                            "source_unit": metric["source_unit"],
                            "value_standardized": standardized,
                            "standardized_unit": metric["standardized_unit"],
                            "quality_status": quality,
                            "source_table": spec["table_code"],
                            "source_filename": filename,
                            "bronze_object_key": object_key,
                            "bronze_record_id": source_row["record_id"],
                            "bronze_record_position": source_row["record_position"],
                            "object_sha256": source_row["object_sha256"],
                        })

            elif spec["layout"] == "YEAR_BY_METRIC":
                # Supports both plain years (2015) and preliminary labels
                # such as "Sơ bộ 2025" used by the source DOCX.
                year_text = str(cells[0]).strip()
                year, preliminary = parse_year_header(year_text)
                geography_name = spec["geography_name"]

                for metric in spec["metrics"]:
                    column_index = metric["column_index"]
                    if column_index >= len(cells):
                        raise RuntimeError(
                            f"{filename}: row at Bronze record_position="
                            f"{source_row['record_position']} has {len(cells)} "
                            f"physical cells, but metric {metric['metric_code']} "
                            f"requires column index {column_index}. "
                            "Inspect the DOCX merged-cell layout."
                        )
                    raw_value = str(cells[column_index]).strip()
                    parsed = parse_vietnamese_decimal(raw_value)
                    force_review = metric.get("force_review", False)
                    if force_review:
                        quality = "REVIEW_DUPLICATE_HEADER"
                    elif raw_value.upper() in MISSING_MARKERS:
                        quality = "MISSING_SOURCE_VALUE"
                    elif parsed is None:
                        quality = "INVALID_NUMERIC_VALUE"
                    else:
                        quality = "PASS"

                    standardized = (
                        parsed * metric["multiplier"]
                        if parsed is not None else None
                    )
                    key = (
                        f"{source_row['record_id']}|{year}|"
                        f"{metric['metric_code']}"
                    )
                    output.append({
                        "silver_record_id": sha256_text(key),
                        "year": year,
                        "is_preliminary": preliminary,
                        "geography_level": "COUNTRY",
                        "geography_name": geography_name,
                        "metric_code": metric["metric_code"],
                        "metric_label": metric["metric_label"],
                        "source_column_index": column_index,
                        "raw_value": raw_value,
                        "value_source": parsed,
                        "source_unit": metric["source_unit"],
                        "value_standardized": standardized,
                        "standardized_unit": metric["standardized_unit"],
                        "quality_status": quality,
                        "source_table": spec["table_code"],
                        "source_filename": filename,
                        "bronze_object_key": object_key,
                        "bronze_record_id": source_row["record_id"],
                        "bronze_record_position": source_row["record_position"],
                        "object_sha256": source_row["object_sha256"],
                    })

            elif spec["layout"] == "GEOGRAPHY_METRICS_SINGLE_YEAR":
                geography_name = str(cells[0]).strip()
                if not geography_name:
                    raise RuntimeError(
                        f"Missing geography in {filename}, "
                        f"record_position={source_row['record_position']}"
                    )
                for metric in spec["metrics"]:
                    column_index = metric["column_index"]
                    raw_value = str(cells[column_index]).strip()
                    parsed = parse_vietnamese_decimal(raw_value)
                    if raw_value.upper() in MISSING_MARKERS:
                        quality = "MISSING_SOURCE_VALUE"
                    elif parsed is None:
                        quality = "INVALID_NUMERIC_VALUE"
                    else:
                        quality = "PASS"

                    standardized = (
                        parsed * metric["multiplier"]
                        if parsed is not None else None
                    )
                    key = (
                        f"{source_row['record_id']}|{spec['year']}|"
                        f"{metric['metric_code']}"
                    )
                    output.append({
                        "silver_record_id": sha256_text(key),
                        "year": spec["year"],
                        "is_preliminary": False,
                        "geography_level": geography_level_for(
                            geography_name, spec["geography_level"]
                        ),
                        "geography_name": geography_name,
                        "metric_code": metric["metric_code"],
                        "metric_label": metric["metric_label"],
                        "source_column_index": column_index,
                        "raw_value": raw_value,
                        "value_source": parsed,
                        "source_unit": metric["source_unit"],
                        "value_standardized": standardized,
                        "standardized_unit": metric["standardized_unit"],
                        "quality_status": quality,
                        "source_table": spec["table_code"],
                        "source_filename": filename,
                        "bronze_object_key": object_key,
                        "bronze_record_id": source_row["record_id"],
                        "bronze_record_position": source_row["record_position"],
                        "object_sha256": source_row["object_sha256"],
                    })
            else:
                raise RuntimeError(
                    f"Unsupported statistics layout: {spec['layout']}"
                )

        per_object[filename] = {
            "bronze_rows_including_header": len(object_rows),
            "data_rows": len(data_rows),
            "silver_observations": sum(
                1 for item in output if item["source_filename"] == filename
            ),
            "table_code": spec["table_code"],
        }

    if len(output) != EXPECTED_STATISTICS_OUTPUT_ROWS:
        raise RuntimeError(
            f"Unexpected Statistics Silver output rows: {len(output)}; "
            f"expected {EXPECTED_STATISTICS_OUTPUT_ROWS}"
        )

    ids = [item["silver_record_id"] for item in output]
    if len(ids) != len(set(ids)):
        raise RuntimeError("Duplicate silver_record_id in Statistics output")

    invalid_numeric = [
        item for item in output
        if item["quality_status"] == "INVALID_NUMERIC_VALUE"
    ]
    if invalid_numeric:
        print("STATISTICS_INVALID_NUMERIC_SAMPLES:", flush=True)
        for item in invalid_numeric[:10]:
            print(item, flush=True)
        raise RuntimeError(
            f"Statistics has {len(invalid_numeric)} invalid numeric values"
        )

    print("STATISTICS_BRONZE_ROWS:", total, flush=True)
    print("STATISTICS_BRONZE_UNIQUE_RECORD_IDS:", unique, flush=True)
    print("STATISTICS_SILVER_ROWS:", len(output), flush=True)
    print("STATISTICS_SILVER_UNIQUE_KEYS:", len(set(ids)), flush=True)
    print("STATISTICS_SOURCE_OBJECT_SUMMARY:", flush=True)
    for filename, summary in sorted(per_object.items()):
        print(
            "STATISTICS_OBJECT:",
            json.dumps({"filename": filename, **summary}, ensure_ascii=False),
            flush=True,
        )

    quality_counts = Counter(item["quality_status"] for item in output)
    print("STATISTICS_QUALITY_COUNTS:", json.dumps(quality_counts, ensure_ascii=False), flush=True)
    print(
        "STATISTICS_MISSING_SOURCE_VALUES:",
        quality_counts.get("MISSING_SOURCE_VALUE", 0),
        flush=True,
    )
    print(
        "STATISTICS_DUPLICATE_HEADER_REVIEW_ROWS:",
        quality_counts.get("REVIEW_DUPLICATE_HEADER", 0),
        flush=True,
    )

    schema = StructType([
        StructField("silver_record_id", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("is_preliminary", BooleanType(), False),
        StructField("geography_level", StringType(), False),
        StructField("geography_name", StringType(), False),
        StructField("metric_code", StringType(), False),
        StructField("metric_label", StringType(), True),
        StructField("source_column_index", IntegerType(), False),
        StructField("raw_value", StringType(), True),
        StructField("value_source", DecimalType(24, 6), True),
        StructField("source_unit", StringType(), True),
        StructField("value_standardized", DecimalType(24, 6), True),
        StructField("standardized_unit", StringType(), True),
        StructField("quality_status", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("source_filename", StringType(), False),
        StructField("bronze_object_key", StringType(), False),
        StructField("bronze_record_id", StringType(), False),
        StructField("bronze_record_position", LongType(), False),
        StructField("object_sha256", StringType(), False),
    ])

    dataframe = spark.createDataFrame(
        [tuple(item[field.name] for field in schema.fields) for item in output],
        schema=schema,
    ).withColumn("silver_processed_at", F.current_timestamp())

    return dataframe


def build_psd_silver(spark):
    bronze = spark.table(PSD_BRONZE)

    required = {
        "record_id", "object_key", "record_position", "domain",
        "source", "record_type", "object_sha256", "raw_record_json",
    }
    missing = required - set(bronze.columns)
    if missing:
        raise RuntimeError(
            f"Coffee PSD Bronze schema missing columns: {sorted(missing)}"
        )

    total = bronze.count()
    unique = bronze.select("record_id").distinct().count()
    if total != EXPECTED_PSD_BRONZE_ROWS or unique != total:
        raise RuntimeError(
            f"Unexpected PSD Bronze counts: rows={total}, unique={unique}"
        )

    type_counts = {
        row["record_type"]: row["count"]
        for row in bronze.groupBy("record_type").count().collect()
    }
    expected_types = {
        "usda_psd_long_csv_row": EXPECTED_PSD_LONG_ROWS,
        "usda_psd_wide_csv_row": EXPECTED_PSD_WIDE_ROWS,
    }
    if type_counts != expected_types:
        raise RuntimeError(
            f"Unexpected PSD record type distribution: {type_counts}"
        )

    long_schema = StructType([
        StructField("source_filename", StringType(), True),
        StructField("record_type", StringType(), True),
        StructField("record_position", LongType(), True),
        StructField("extracted_record", StructType([
            StructField("row_index", LongType(), True),
            StructField("columns", ArrayType(StringType()), True),
            StructField("values", MapType(StringType(), StringType()), True),
        ]), True),
    ])

    common = bronze.select(
        "record_id", "object_key", "record_position", "object_sha256",
        "record_type", "raw_record_json",
    ).withColumn("_payload", F.from_json("raw_record_json", long_schema))

    long_rows = common.filter(
        F.col("record_type") == "usda_psd_long_csv_row"
    )
    long_values = F.col("_payload.extracted_record.values")

    long_base = long_rows.select(
        F.col("record_id").alias("bronze_record_id"),
        F.col("object_key").alias("bronze_object_key"),
        F.col("record_position").alias("bronze_record_position"),
        F.col("object_sha256"),
        F.col("_payload.source_filename").alias("source_filename"),
        F.lit("LONG_CSV").alias("source_format"),
        F.element_at(long_values, F.lit("Commodity_Code")).alias("commodity_code"),
        F.element_at(long_values, F.lit("Commodity_Description")).alias("commodity_description"),
        F.element_at(long_values, F.lit("Country_Code")).alias("country_code"),
        F.element_at(long_values, F.lit("Country_Name")).alias("country_name"),
        F.element_at(long_values, F.lit("Market_Year")).alias("market_year"),
        F.element_at(long_values, F.lit("Calendar_Year")).cast("int").alias("calendar_year"),
        F.element_at(long_values, F.lit("Month")).cast("int").alias("month"),
        F.element_at(long_values, F.lit("Attribute_ID")).alias("attribute_id"),
        F.element_at(long_values, F.lit("Attribute_Description")).alias("attribute_description"),
        F.element_at(long_values, F.lit("Unit_ID")).alias("unit_id"),
        F.element_at(long_values, F.lit("Unit_Description")).alias("unit_description"),
        F.element_at(long_values, F.lit("Value")).alias("raw_value"),
    )

    long_cleaned = long_base.withColumn(
        "_clean_value",
        F.when(
            F.upper(F.trim("raw_value")).isin(*sorted(MISSING_MARKERS)),
            F.lit(None).cast("string"),
        ).otherwise(F.trim("raw_value")),
    ).withColumn(
        "value",
        parse_psd_decimal_expression(F.col("_clean_value")),
    ).withColumn(
        "quality_status",
        F.when(F.col("_clean_value").isNull(), "MISSING_SOURCE_VALUE")
         .when(F.col("value").isNull(), "INVALID_NUMERIC_VALUE")
         .otherwise("PASS"),
    ).withColumn(
        "market_year_start",
        F.regexp_extract("market_year", r"^((?:19|20)[0-9]{2})", 1).cast("int"),
    ).withColumn(
        "market_year_end",
        F.regexp_extract("market_year", r"^[0-9]{4}/([0-9]{4})$", 1).cast("int"),
    ).withColumn(
        "silver_record_id",
        F.sha2(
            F.concat_ws(
                "||", F.col("bronze_record_id"), F.col("source_format"),
                F.coalesce(F.col("market_year"), F.lit("")),
            ),
            256,
        ),
    )

    wide_schema = StructType([
        StructField("source_filename", StringType(), True),
        StructField("record_type", StringType(), True),
        StructField("record_position", LongType(), True),
        StructField("extracted_record", StructType([
            StructField("row_index", LongType(), True),
            StructField("columns", ArrayType(StringType()), True),
            StructField("values", MapType(StringType(), StringType()), True),
        ]), True),
    ])

    wide_rows = common.filter(
        F.col("record_type") == "usda_psd_wide_csv_row"
    ).withColumn(
        "_year_columns",
        F.expr(
            "filter(_payload.extracted_record.columns, "
            "x -> x RLIKE '^[0-9]{4}/[0-9]{4}$')"
        ),
    )

    # The USDA wide export uses a visually merged Commodity cell: the first
    # CSV row contains "Coffee, Green", while following rows leave Commodity
    # blank. Carry the last non-empty value forward within each source object.
    # Partitioning by object_key prevents values leaking between files.
    wide_order = (
        Window.partitionBy("object_key")
        .orderBy("record_position")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    wide_rows = wide_rows.withColumn(
        "_commodity_raw",
        F.when(
            F.length(
                F.trim(
                    F.element_at(
                        F.col("_payload.extracted_record.values"),
                        F.lit("Commodity"),
                    )
                )
            ) > 0,
            F.trim(
                F.element_at(
                    F.col("_payload.extracted_record.values"),
                    F.lit("Commodity"),
                )
            ),
        ),
    ).withColumn(
        "_commodity_filled",
        F.last("_commodity_raw", ignorenulls=True).over(wide_order),
    )

    commodity_forward_filled_rows = wide_rows.filter(
        F.col("_commodity_raw").isNull()
        & F.col("_commodity_filled").isNotNull()
    ).count()
    print(
        "PSD_WIDE_COMMODITY_FORWARD_FILLED_ROWS:",
        commodity_forward_filled_rows,
        flush=True,
    )

    year_counts = [
        int(row["year_count"])
        for row in wide_rows.select(
            F.size("_year_columns").alias("year_count")
        ).distinct().collect()
    ]
    if year_counts != [EXPECTED_PSD_WIDE_YEAR_COLUMNS]:
        raise RuntimeError(
            "Unexpected PSD wide year columns per row: "
            f"{year_counts}; expected {[EXPECTED_PSD_WIDE_YEAR_COLUMNS]}"
        )

    wide_exploded = wide_rows.withColumn(
        "_market_year", F.explode("_year_columns")
    )
    wide_values = F.col("_payload.extracted_record.values")

    wide_base = wide_exploded.select(
        F.col("record_id").alias("bronze_record_id"),
        F.col("object_key").alias("bronze_object_key"),
        F.col("record_position").alias("bronze_record_position"),
        F.col("object_sha256"),
        F.col("_payload.source_filename").alias("source_filename"),
        F.lit("WIDE_CSV").alias("source_format"),
        F.lit(None).cast("string").alias("commodity_code"),
        F.col("_commodity_filled").alias("commodity_description"),
        F.lit(None).cast("string").alias("country_code"),
        F.element_at(wide_values, F.lit("Country")).alias("country_name"),
        F.col("_market_year").alias("market_year"),
        F.lit(None).cast("int").alias("calendar_year"),
        F.lit(None).cast("int").alias("month"),
        F.lit(None).cast("string").alias("attribute_id"),
        F.element_at(wide_values, F.lit("Attribute")).alias("attribute_description"),
        F.lit(None).cast("string").alias("unit_id"),
        F.coalesce(
            F.element_at(wide_values, F.lit("Unit Description")),
            F.element_at(wide_values, F.lit("Unit_Description")),
        ).alias("unit_description"),
        F.element_at(wide_values, F.col("_market_year")).alias("raw_value"),
    )

    wide_cleaned = wide_base.withColumn(
        "_clean_value",
        F.when(
            F.upper(F.trim("raw_value")).isin(*sorted(MISSING_MARKERS)),
            F.lit(None).cast("string"),
        ).otherwise(F.trim("raw_value")),
    ).withColumn(
        "value",
        parse_psd_decimal_expression(F.col("_clean_value")),
    ).withColumn(
        "quality_status",
        F.when(F.col("_clean_value").isNull(), "MISSING_SOURCE_VALUE")
         .when(F.col("value").isNull(), "INVALID_NUMERIC_VALUE")
         .otherwise("PASS"),
    ).withColumn(
        "market_year_start",
        F.substring("market_year", 1, 4).cast("int"),
    ).withColumn(
        "market_year_end",
        F.substring("market_year", 6, 4).cast("int"),
    ).withColumn(
        "silver_record_id",
        F.sha2(
            F.concat_ws(
                "||", F.col("bronze_record_id"), F.col("source_format"),
                F.col("market_year"),
            ),
            256,
        ),
    )

    output_columns = [
        "silver_record_id", "source_format", "commodity_code",
        "commodity_description", "country_code", "country_name",
        "market_year", "market_year_start", "market_year_end",
        "calendar_year", "month", "attribute_id", "attribute_description",
        "unit_id", "unit_description", "raw_value", "value",
        "quality_status", "source_filename", "bronze_object_key",
        "bronze_record_id", "bronze_record_position", "object_sha256",
    ]

    result = (
        long_cleaned.select(*output_columns)
        .unionByName(wide_cleaned.select(*output_columns))
        .withColumn("silver_processed_at", F.current_timestamp())
        .cache()
    )

    output_rows = result.count()
    unique_keys = result.select("silver_record_id").distinct().count()

    required_field_conditions = {
        "silver_record_id": F.col("silver_record_id").isNull()
            | (F.trim("silver_record_id") == ""),
        "commodity_description": F.col("commodity_description").isNull()
            | (F.trim("commodity_description") == ""),
        "country_name": F.col("country_name").isNull()
            | (F.trim("country_name") == ""),
        "attribute_description": F.col("attribute_description").isNull()
            | (F.trim("attribute_description") == ""),
        "unit_description": F.col("unit_description").isNull()
            | (F.trim("unit_description") == ""),
        "market_year": F.col("market_year").isNull()
            | (F.trim("market_year") == ""),
        "bronze_object_key": F.col("bronze_object_key").isNull()
            | (F.trim("bronze_object_key") == ""),
        "bronze_record_id": F.col("bronze_record_id").isNull()
            | (F.trim("bronze_record_id") == ""),
    }
    required_violation_counts = {
        field: result.filter(condition).count()
        for field, condition in required_field_conditions.items()
    }
    invalid_required = result.filter(
        reduce(lambda left, right: left | right,
               required_field_conditions.values())
    ).count()

    invalid_numeric = result.filter(
        F.col("quality_status") == "INVALID_NUMERIC_VALUE"
    ).count()
    missing_values = result.filter(
        F.col("quality_status") == "MISSING_SOURCE_VALUE"
    ).count()

    print("PSD_BRONZE_ROWS:", total, flush=True)
    print("PSD_BRONZE_UNIQUE_RECORD_IDS:", unique, flush=True)
    print("PSD_RECORD_TYPE_COUNTS:", json.dumps(type_counts), flush=True)
    print("PSD_WIDE_YEAR_COLUMNS_PER_ROW:", year_counts, flush=True)
    print("PSD_LONG_SILVER_ROWS:", long_cleaned.count(), flush=True)
    print("PSD_WIDE_EXPANDED_SILVER_ROWS:", wide_cleaned.count(), flush=True)
    print("PSD_SILVER_ROWS:", output_rows, flush=True)
    print("PSD_SILVER_UNIQUE_KEYS:", unique_keys, flush=True)
    print("PSD_INVALID_REQUIRED_ROWS:", invalid_required, flush=True)
    print(
        "PSD_REQUIRED_FIELD_VIOLATION_COUNTS:",
        json.dumps(required_violation_counts, ensure_ascii=False),
        flush=True,
    )
    print("PSD_INVALID_NUMERIC_ROWS:", invalid_numeric, flush=True)
    print("PSD_MISSING_SOURCE_VALUES:", missing_values, flush=True)
    print("PSD_QUALITY_COUNTS:", flush=True)
    result.groupBy("quality_status").count().orderBy("quality_status").show(
        truncate=False
    )
    print("PSD_SAMPLE:", flush=True)
    result.select(
        "source_format", "commodity_description", "country_name",
        "market_year", "attribute_description", "unit_description",
        "raw_value", "value", "quality_status",
    ).orderBy("source_format", "market_year").show(12, truncate=False)

    if output_rows != EXPECTED_PSD_OUTPUT_ROWS:
        raise RuntimeError(
            f"Unexpected PSD Silver rows: {output_rows}; "
            f"expected {EXPECTED_PSD_OUTPUT_ROWS}"
        )
    if unique_keys != output_rows:
        raise RuntimeError("Duplicate silver_record_id in PSD Silver output")
    if invalid_required:
        raise RuntimeError(
            f"PSD Silver has {invalid_required} invalid required rows"
        )
    if invalid_numeric:
        result.filter(
            F.col("quality_status") == "INVALID_NUMERIC_VALUE"
        ).select(
            "source_filename", "bronze_record_position", "raw_value",
            "commodity_description", "country_name", "attribute_description",
        ).show(30, truncate=False)
        raise RuntimeError(
            f"PSD Silver has {invalid_numeric} invalid numeric values"
        )

    return result


def write_iceberg(spark, target: str, dataframe, business_key: str, label: str):
    """Idempotent insert-only MERGE; never overwrite existing Silver rows."""
    view = "stage_" + target.split(".")[-1]
    dataframe.createOrReplaceTempView(view)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} "
        f"USING iceberg AS SELECT * FROM {view} WHERE 1 = 0"
    )

    existing = spark.table(target)
    if set(existing.columns) != set(dataframe.columns):
        raise RuntimeError(
            f"{target} schema differs from current output; no write attempted"
        )

    overlap = existing.alias("t").join(
        dataframe.alias("s"),
        F.col(f"t.{business_key}") == F.col(f"s.{business_key}"),
        "inner",
    )

    logical_columns = [
        column for column in dataframe.columns
        if column != "silver_processed_at"
    ]
    mismatch = None
    for column in logical_columns:
        condition = ~F.col(f"t.{column}").eqNullSafe(F.col(f"s.{column}"))
        mismatch = condition if mismatch is None else mismatch | condition

    mismatch_count = overlap.filter(mismatch).count()
    if mismatch_count:
        raise RuntimeError(
            f"{target} has {mismatch_count} existing keys with changed values; "
            "inspect before updating"
        )

    spark.sql(f"""
        MERGE INTO {target} AS target
        USING {view} AS source
        ON target.{business_key} = source.{business_key}
        WHEN NOT MATCHED THEN INSERT *
    """)

    stored = spark.table(target)
    total = stored.count()
    unique = stored.select(business_key).distinct().count()
    expected = dataframe.count()

    print(f"{label}_ICEBERG_TOTAL:", total, flush=True)
    print(f"{label}_ICEBERG_UNIQUE_KEYS:", unique, flush=True)

    if total != expected or unique != total:
        raise RuntimeError(
            f"{target} write verification failed: "
            f"stored={total}, unique={unique}, expected={expected}"
        )

    print(f"{label}_ICEBERG_PASSED", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write validated Statistics and PSD Silver tables to Iceberg.",
    )
    args = parser.parse_args()

    spark = make_spark()
    statistics_df = None
    psd_df = None

    try:
        statistics_df = build_statistics_rows(spark).cache()
        psd_df = build_psd_silver(spark)

        statistics_count = statistics_df.count()
        statistics_unique = statistics_df.select(
            "silver_record_id"
        ).distinct().count()
        statistics_invalid_required = statistics_df.filter(
            F.col("silver_record_id").isNull()
            | F.col("year").isNull()
            | F.col("geography_name").isNull()
            | F.col("metric_code").isNull()
            | F.col("bronze_object_key").isNull()
            | F.col("bronze_record_id").isNull()
        ).count()

        print("===== STATISTICS SILVER VALIDATION =====", flush=True)
        print("STATISTICS_SILVER_ROWS:", statistics_count, flush=True)
        print("STATISTICS_SILVER_UNIQUE_KEYS:", statistics_unique, flush=True)
        print(
            "STATISTICS_INVALID_REQUIRED_ROWS:",
            statistics_invalid_required,
            flush=True,
        )
        print("STATISTICS_QUALITY_COUNTS:", flush=True)
        statistics_df.groupBy("quality_status").count().orderBy(
            "quality_status"
        ).show(truncate=False)
        print("STATISTICS_SAMPLE:", flush=True)
        statistics_df.select(
            "year", "is_preliminary", "geography_level", "geography_name",
            "metric_code", "raw_value", "value_standardized",
            "standardized_unit", "quality_status",
        ).orderBy("source_table", "year", "geography_name").show(
            20, truncate=False
        )

        if statistics_count != EXPECTED_STATISTICS_OUTPUT_ROWS:
            raise RuntimeError(
                f"Unexpected Statistics Silver count: {statistics_count}"
            )
        if statistics_unique != statistics_count:
            raise RuntimeError("Duplicate Statistics Silver business keys")
        if statistics_invalid_required:
            raise RuntimeError(
                f"Statistics has {statistics_invalid_required} invalid required rows"
            )

        print("SILVER_COFFEE_STATISTICS_DRY_RUN_PASSED", flush=True)
        print("SILVER_COFFEE_PSD_DRY_RUN_PASSED", flush=True)
        print("SILVER_COFFEE_NEW_DRY_RUN_PASSED", flush=True)

        if args.write:
            write_iceberg(
                spark,
                STATISTICS_SILVER,
                statistics_df,
                "silver_record_id",
                "COFFEE_STATISTICS",
            )
            write_iceberg(
                spark,
                PSD_SILVER,
                psd_df,
                "silver_record_id",
                "COFFEE_PSD",
            )
            print("SILVER_COFFEE_NEW_ICEBERG_PASSED", flush=True)
        else:
            print("NO_SILVER_TABLES_WRITTEN", flush=True)

    finally:
        if statistics_df is not None:
            statistics_df.unpersist()
        if psd_df is not None:
            psd_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
