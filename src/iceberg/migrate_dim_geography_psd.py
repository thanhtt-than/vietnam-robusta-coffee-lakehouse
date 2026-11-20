# -*- coding: utf-8 -*-
"""
Migrate DIM_GEOGRAPHY with USDA PSD reporting geographies.

Default mode: DRY RUN (no Iceberg writes).
Write mode: pass --write through run_spark.ps1.

Design:
- Preserve all existing DIM_GEOGRAPHY rows and surrogate keys.
- Use LONG_CSV as the authoritative PSD country/reporting-geography list.
- Match existing rows by normalized geography name.
- Map "Vietnam" from PSD to the existing "Viet Nam" row.
- Add only missing reporting geographies with new surrogate keys.
- Keep source-specific identifiers in namespaced geography_code values
  (PSD_<country_code>) for newly inserted rows.
- Classify European Union as REGION; other PSD country entities as COUNTRY.
"""

import re
import sys
import unicodedata
from collections import defaultdict

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)


WRITE = "--write" in sys.argv[1:]
PSD_TABLE = "lakehouse.silver.coffee_psd_observations"
TARGET = "lakehouse.gold.dim_geography"

EXPECTED_COLUMNS = [
    "geography_key",
    "geography_code",
    "geography_name",
    "geography_level",
    "region_name",
]

# Explicit source-name aliases. Extend this dictionary only when a source
# name is confirmed to refer to an existing geography entity.
NAME_ALIASES = {
    "vietnam": "viet nam",
}

EUROPEAN_UNION_KEY = "european union"


def normalize_text(value):
    """Normalize a name for matching; do not overwrite its display value."""
    if value is None:
        return None

    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(
        ch for ch in text
        if not unicodedata.combining(ch)
    )
    text = text.casefold().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def canonical_name(value):
    normalized = normalize_text(value)
    if normalized is None:
        return None
    return NAME_ALIASES.get(normalized, normalized)


def safe_source_code(value):
    """Create a stable, namespaced geography_code from a PSD code."""
    code = re.sub(
        r"[^A-Z0-9]+",
        "_",
        str(value).strip().upper()
    ).strip("_")

    if not code:
        raise ValueError(
            "PSD country_code becomes empty after normalization"
        )

    return "PSD_" + code


def validate_dimension_rows(rows):
    """Validate current dimension keys and natural codes."""
    keys = [r["geography_key"] for r in rows]
    codes = [r["geography_code"] for r in rows]

    if any(value is None for value in keys):
        raise RuntimeError(
            "DIM_GEOGRAPHY contains NULL geography_key"
        )

    if any(value is None or not str(value).strip() for value in codes):
        raise RuntimeError(
            "DIM_GEOGRAPHY contains NULL/empty geography_code"
        )

    if len(keys) != len(set(keys)):
        raise RuntimeError(
            "DIM_GEOGRAPHY contains duplicate geography_key"
        )

    if len(codes) != len(set(codes)):
        raise RuntimeError(
            "DIM_GEOGRAPHY contains duplicate geography_code"
        )


spark = (
    SparkSession.builder
    .appName("TLCN-Migrate-DIM-Geography-PSD")
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
    # ------------------------------------------------------------------
    # 1. Read and validate the current Gold dimension.
    # ------------------------------------------------------------------
    if not spark.catalog.tableExists(TARGET):
        raise RuntimeError(
            "Required Gold table does not exist: " + TARGET
        )

    dim_df = spark.table(TARGET)

    if dim_df.columns != EXPECTED_COLUMNS:
        raise RuntimeError(
            "Unexpected DIM_GEOGRAPHY schema. Expected "
            + str(EXPECTED_COLUMNS)
            + ", got "
            + str(dim_df.columns)
        )

    existing_rows = dim_df.collect()
    validate_dimension_rows(existing_rows)

    existing_count = len(existing_rows)
    max_key = max(
        int(r["geography_key"]) for r in existing_rows
    )

    existing_country_rows = [
        r for r in existing_rows
        if r["geography_level"] == "COUNTRY"
    ]

    # Include REGION so a source-level aggregate such as European Union
    # can be matched to a reporting region rather than duplicated.
    existing_region_rows = [
        r for r in existing_rows
        if r["geography_level"] == "REGION"
    ]

    existing_match_rows = (
        existing_country_rows + existing_region_rows
    )

    existing_by_name = defaultdict(list)

    for row in existing_match_rows:
        key = canonical_name(row["geography_name"])
        if key:
            existing_by_name[key].append(row)

    duplicate_existing_names = {
        key: rows
        for key, rows in existing_by_name.items()
        if len(rows) > 1
    }

    if duplicate_existing_names:
        details = {
            key: [
                (
                    r["geography_key"],
                    r["geography_code"],
                    r["geography_name"],
                    r["geography_level"],
                )
                for r in rows
            ]
            for key, rows in duplicate_existing_names.items()
        }
        raise RuntimeError(
            "Ambiguous existing geography names: " + str(details)
        )

    print(
        "DIM_GEOGRAPHY_EXISTING_ROWS:",
        existing_count,
        flush=True
    )
    print(
        "DIM_GEOGRAPHY_EXISTING_MAX_KEY:",
        max_key,
        flush=True
    )
    print(
        "DIM_GEOGRAPHY_EXISTING_COUNTRY_ROWS:",
        len(existing_country_rows),
        flush=True
    )
    print(
        "DIM_GEOGRAPHY_EXISTING_REGION_ROWS:",
        len(existing_region_rows),
        flush=True
    )

    # ------------------------------------------------------------------
    # 2. Read the PSD reporting-geography list.
    #    LONG_CSV is used to avoid counting the overlapping WIDE_CSV
    #    expansion as a second source of geography entities.
    # ------------------------------------------------------------------
    psd = spark.table(PSD_TABLE)

    required_psd_columns = {
        "source_format",
        "country_code",
        "country_name",
    }

    missing_columns = (
        required_psd_columns - set(psd.columns)
    )

    if missing_columns:
        raise RuntimeError(
            "PSD Silver is missing required columns: "
            + str(sorted(missing_columns))
        )

    psd_rows = (
        psd
        .filter(F.col("source_format") == "LONG_CSV")
        .select("country_code", "country_name")
        .distinct()
        .collect()
    )

    if not psd_rows:
        raise RuntimeError(
            "No PSD LONG_CSV country records found"
        )

    source_entities = []

    for row in psd_rows:
        source_code = (
            str(row["country_code"]).strip()
            if row["country_code"] is not None
            else ""
        )
        source_name = (
            str(row["country_name"]).strip()
            if row["country_name"] is not None
            else ""
        )

        if not source_code or not source_name:
            raise RuntimeError(
                "PSD contains NULL/empty country_code or country_name"
            )

        source_entities.append(
            (source_code, source_name, canonical_name(source_name))
        )

    # A source code must identify exactly one reporting-geography name.
    names_by_source_code = defaultdict(set)

    for source_code, source_name, normalized_name in source_entities:
        names_by_source_code[source_code].add(normalized_name)

    conflicting_source_codes = {
        code: sorted(names)
        for code, names in names_by_source_code.items()
        if len(names) != 1
    }

    if conflicting_source_codes:
        raise RuntimeError(
            "PSD country_code maps to multiple names: "
            + str(conflicting_source_codes)
        )

    # A normalized name may have multiple source codes. In that case,
    # keep one dimension row and report the aliases; the Fact can resolve
    # the source row by normalized country_name.
    codes_by_name = defaultdict(set)
    display_names_by_name = defaultdict(set)

    for source_code, source_name, normalized_name in source_entities:
        codes_by_name[normalized_name].add(source_code)
        display_names_by_name[normalized_name].add(source_name)

    # ------------------------------------------------------------------
    # 3. Identify missing entities without changing existing rows.
    # ------------------------------------------------------------------
    new_entities = []

    for normalized_name in sorted(codes_by_name):
        if normalized_name in existing_by_name:
            continue

        source_codes = sorted(codes_by_name[normalized_name])
        source_names = sorted(display_names_by_name[normalized_name])

        # Use the lexicographically first source code as the stable
        # representative if the source has aliases for one name.
        representative_code = source_codes[0]
        representative_name = source_names[0]

        geography_code = safe_source_code(
            representative_code
        )

        if geography_code in {
            str(r["geography_code"]) for r in existing_rows
        }:
            raise RuntimeError(
                "Generated geography_code already exists: "
                + geography_code
            )

        if normalized_name == EUROPEAN_UNION_KEY:
            geography_level = "REGION"
            region_name = "EUROPEAN_UNION"
        else:
            geography_level = "COUNTRY"
            region_name = None

        new_entities.append(
            (
                representative_code,
                representative_name,
                geography_code,
                geography_level,
                region_name,
                normalized_name,
                len(source_codes),
            )
        )

    # Allocate new surrogate keys after the current maximum.
    # Sorting by geography_code makes allocation deterministic for this
    # complete migration batch.
    new_entities.sort(key=lambda x: x[2])

    planned_rows = [
        (
            max_key + index,
            entity[2],
            entity[1],
            entity[3],
            entity[4],
        )
        for index, entity in enumerate(new_entities, start=1)
    ]

    planned_schema = StructType([
        StructField("geography_key", IntegerType(), False),
        StructField("geography_code", StringType(), False),
        StructField("geography_name", StringType(), False),
        StructField("geography_level", StringType(), False),
        StructField("region_name", StringType(), True),
    ])

    planned_df = spark.createDataFrame(
        planned_rows,
        schema=planned_schema
    )

    source_name_keys = set(codes_by_name.keys())
    existing_name_keys = set(existing_by_name.keys())
    planned_name_keys = {
        entity[5] for entity in new_entities
    }

    already_mapped = source_name_keys & existing_name_keys
    still_unmapped = (
        source_name_keys
        - existing_name_keys
        - planned_name_keys
    )

    print(
        "PSD_DISTINCT_SOURCE_CODES:",
        len(names_by_source_code),
        flush=True
    )
    print(
        "PSD_DISTINCT_REPORTING_GEOGRAPHIES:",
        len(source_name_keys),
        flush=True
    )
    print(
        "PSD_GEOGRAPHY_ALREADY_MAPPED:",
        len(already_mapped),
        flush=True
    )
    print(
        "PSD_GEOGRAPHY_TO_INSERT:",
        len(planned_rows),
        flush=True
    )
    print(
        "PSD_GEOGRAPHY_UNMAPPED:",
        len(still_unmapped),
        flush=True
    )

    if still_unmapped:
        raise RuntimeError(
            "PSD geography mapping is incomplete: "
            + str(sorted(still_unmapped))
        )

    # Show the proposed additions before any write.
    if planned_rows:
        print(
            "===== PLANNED DIM_GEOGRAPHY INSERTS =====",
            flush=True
        )
        planned_df.orderBy("geography_key").show(
            200,
            truncate=False
        )
    else:
        print(
            "NO_NEW_GEOGRAPHY_ROWS_REQUIRED",
            flush=True
        )

    # Validate planned rows.
    if planned_df.count() != len(planned_rows):
        raise RuntimeError(
            "Planned row count does not match generated records"
        )

    if (
        planned_df.select("geography_key").distinct().count()
        != len(planned_rows)
    ):
        raise RuntimeError(
            "Planned geography_key values are not unique"
        )

    if (
        planned_df.select("geography_code").distinct().count()
        != len(planned_rows)
    ):
        raise RuntimeError(
            "Planned geography_code values are not unique"
        )

    # ------------------------------------------------------------------
    # 4. Dry run or Iceberg MERGE.
    # ------------------------------------------------------------------
    if WRITE:
        if planned_rows:
            planned_df.createOrReplaceTempView(
                "stage_dim_geography_psd"
            )

            spark.sql(f"""
                MERGE INTO {TARGET} AS t
                USING stage_dim_geography_psd AS s
                ON t.geography_code = s.geography_code
                WHEN NOT MATCHED THEN
                    INSERT (
                        geography_key,
                        geography_code,
                        geography_name,
                        geography_level,
                        region_name
                    )
                    VALUES (
                        s.geography_key,
                        s.geography_code,
                        s.geography_name,
                        s.geography_level,
                        s.region_name
                    )
            """)

        # ------------------------------------------------------------------
        # 5. Post-write validation.
        # ------------------------------------------------------------------
        saved_df = spark.table(TARGET)
        saved_rows = saved_df.collect()

        validate_dimension_rows(saved_rows)

        if len(saved_rows) != existing_count + len(planned_rows):
            raise RuntimeError(
                "DIM_GEOGRAPHY row-count mismatch after MERGE. "
                "Expected "
                + str(existing_count + len(planned_rows))
                + ", got "
                + str(len(saved_rows))
            )

        saved_match_names = defaultdict(list)

        for row in saved_rows:
            if row["geography_level"] in ("COUNTRY", "REGION"):
                key = canonical_name(row["geography_name"])
                if key:
                    saved_match_names[key].append(row)

        post_write_unmapped = (
            source_name_keys - set(saved_match_names.keys())
        )

        ambiguous_after_write = {
            key: rows
            for key, rows in saved_match_names.items()
            if key in source_name_keys and len(rows) != 1
        }

        if post_write_unmapped:
            raise RuntimeError(
                "PSD geographies remain unmapped after MERGE: "
                + str(sorted(post_write_unmapped))
            )

        if ambiguous_after_write:
            raise RuntimeError(
                "PSD geography maps to multiple DIM rows: "
                + str(sorted(ambiguous_after_write.keys()))
            )

        print(
            "DIM_GEOGRAPHY_ICEBERG_TOTAL:",
            len(saved_rows),
            flush=True
        )
        print(
            "DIM_GEOGRAPHY_ICEBERG_UNIQUE_KEYS:",
            len({r["geography_key"] for r in saved_rows}),
            flush=True
        )
        print(
            "DIM_GEOGRAPHY_ICEBERG_UNIQUE_CODES:",
            len({r["geography_code"] for r in saved_rows}),
            flush=True
        )
        print(
            "PSD_GEOGRAPHY_POST_WRITE_UNMAPPED:",
            len(post_write_unmapped),
            flush=True
        )
        print(
            "DIM_GEOGRAPHY_PSD_MIGRATION_PASSED",
            flush=True
        )

    else:
        print(
            "DIM_GEOGRAPHY_PSD_DRY_RUN_PASSED",
            flush=True
        )
        print(
            "NO_GOLD_TABLE_WRITTEN",
            flush=True
        )

finally:
    spark.stop()
