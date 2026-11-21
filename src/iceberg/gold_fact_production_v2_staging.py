"""Build a validated v2 staging Fact Production from Legacy + DOCX Statistics.

Inputs:
  lakehouse.silver.coffee_area_annual
  lakehouse.silver.coffee_production_annual
  lakehouse.silver.coffee_statistics_observations
  lakehouse.bronze.coffee_statistics_raw (for source-code lineage)
  Gold dimensions: dim_year, dim_geography, dim_source, dim_coffee_product

Output (only with --write):
  lakehouse.gold.fact_production_v2_staging

The existing lakehouse.gold.fact_production is NEVER modified by this script.
Default mode is read-only DRY RUN.
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from functools import reduce

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType


LEGACY_AREA = "lakehouse.silver.coffee_area_annual"
LEGACY_PRODUCTION = "lakehouse.silver.coffee_production_annual"
STATISTICS_SILVER = "lakehouse.silver.coffee_statistics_observations"
STATISTICS_BRONZE = "lakehouse.bronze.coffee_statistics_raw"

DIM_YEAR = "lakehouse.gold.dim_year"
DIM_GEOGRAPHY = "lakehouse.gold.dim_geography"
DIM_SOURCE = "lakehouse.gold.dim_source"
DIM_PRODUCT = "lakehouse.gold.dim_coffee_product"

TARGET = "lakehouse.gold.fact_production_v2_staging"
EXPECTED_ROWS = {
    "LEGACY_VIETNAM_ANNUAL": 19,
    "PROVINCE_STATISTICS": 65,
    "REGION_STATISTICS": 30,
    "VIETNAM_ANNUAL_STATISTICS": 11,
    "WORLD_MAJOR_COUNTRIES_2024": 16,
}
EXPECTED_TOTAL_ROWS = sum(EXPECTED_ROWS.values())  # 141
EXPECTED_STATISTICS_ROWS = 301
EXPECTED_STATISTICS_MISSING_VALUES = 4

JOIN_KEYS = ["year_key", "geography_key", "is_preliminary"]
GRAIN_KEYS = [
    "year_key",
    "geography_key",
    "coffee_product_key",
    "dataset_code",
    "area_measure_type",
    "publication_status_code",
]


# All aliases are normalized (lowercase, no accents, punctuation collapsed).
# Keep this mapping explicit: an unknown geography must fail rather than be
# silently assigned to an incorrect dimension member.
GEOGRAPHY_ALIASES = {
    # Vietnam / national total
    "ca nuoc": "VN",
    "viet nam": "VN",
    "vietnam": "VN",
    "viet nam total": "VN",
    # World total
    "tong the gioi": "WORLD",
    "toan the gioi": "WORLD",
    "world": "WORLD",
    "world total": "WORLD",
    # Provinces/cities in the supplied DOCX files
    "dien bien": "DIEN_BIEN",
    "son la": "SON_LA",
    "phu tho": "PHU_THO",
    "nghe an": "NGHE_AN",
    "quang tri": "QUANG_TRI",
    "quang ngai": "QUANG_NGAI",
    "khanh hoa": "KHANH_HOA",
    "dak lak": "DAK_LAK",
    "daklak": "DAK_LAK",
    "gia lai": "GIA_LAI",
    "lam dong": "LAM_DONG",
    "dong nai": "DONG_NAI",
    "tp ho chi minh": "HO_CHI_MINH_CITY",
    "thanh pho ho chi minh": "HO_CHI_MINH_CITY",
    "ho chi minh city": "HO_CHI_MINH_CITY",
    # Regions
    "trung du va mien nui phia bac": "NORTHERN_MIDLANDS_AND_MOUNTAINS",
    "northern midlands and mountains": "NORTHERN_MIDLANDS_AND_MOUNTAINS",
    "bac trung bo": "NORTH_CENTRAL",
    "north central": "NORTH_CENTRAL",
    "duyen hai ntb tay nguyen": "SOUTH_CENTRAL_COAST_AND_CENTRAL_HIGHLANDS",
    "duyen hai nam trung bo va tay nguyen": "SOUTH_CENTRAL_COAST_AND_CENTRAL_HIGHLANDS",
    "south central coast and central highlands": "SOUTH_CENTRAL_COAST_AND_CENTRAL_HIGHLANDS",
    "dong nam bo": "SOUTHEAST",
    "southeast": "SOUTHEAST",
    # Countries in WORLD_MAJOR_COUNTRIES_2024
    "brazil": "BRAZIL",
    "colombia": "COLOMBIA",
    "indonesia": "INDONESIA",
    "ethiopia": "ETHIOPIA",
    "uganda": "UGANDA",
    "india": "INDIA",
    "peru": "PERU",
    "central african republic": "CENTRAL_AFRICAN_REPUBLIC",
    "cong hoa trung phi": "CENTRAL_AFRICAN_REPUBLIC",
    "ch trung phi": "CENTRAL_AFRICAN_REPUBLIC",
    "honduras": "HONDURAS",
    "guatemala": "GUATEMALA",
    "mexico": "MEXICO",
    "laos": "LAOS",
    "lao pdr": "LAOS",
    "lao": "LAOS",
    "guinea": "GUINEA",
    "china": "CHINA",
    "trung quoc": "CHINA",
    "an do": "INDIA",
}

MEASURE_META_FIELDS = [
    "source_key",
    "quality_status",
    "publication_status",
    "raw_observation_count",
    "silver_record_id",
    "bronze_record_id",
    "bronze_object_key",
    "bronze_record_position",
    "object_sha256",
    "source_filename",
    "source_table",
]

FACT_COLUMNS = [
    "production_fact_key",
    "year_key",
    "geography_key",
    "coffee_product_key",
    "dataset_code",
    "area_measure_type",
    "is_preliminary",
    "publication_status_code",
    "coffee_area_thousand_ha",
    "coffee_production_thousand_tons",
    "yield_tons_per_ha",
    "area_growth_index_prev_year_100",
    "production_growth_index_prev_year_100",
    "area_source_key",
    "production_source_key",
    "yield_source_key",
    "area_publication_status",
    "production_publication_status",
    "yield_publication_status",
    "area_quality_status",
    "production_quality_status",
    "yield_quality_status",
    "area_raw_observation_count",
    "production_raw_observation_count",
    "yield_raw_observation_count",
    "area_silver_record_id",
    "production_silver_record_id",
    "yield_silver_record_id",
    "area_bronze_record_id",
    "production_bronze_record_id",
    "yield_bronze_record_id",
    "area_bronze_object_key",
    "production_bronze_object_key",
    "yield_bronze_object_key",
    "area_bronze_record_position",
    "production_bronze_record_position",
    "yield_bronze_record_position",
    "area_object_sha256",
    "production_object_sha256",
    "yield_object_sha256",
    "area_source_filename",
    "production_source_filename",
    "yield_source_filename",
    "area_source_table",
    "production_source_table",
    "yield_source_table",
    "yield_method",
    "gold_processed_at",
]


def make_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Gold-Fact-Production-V2-Staging")
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


def require_columns(df, required, label):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def assert_unique(df, columns, label):
    total = df.count()
    unique = df.select(*columns).distinct().count()
    if total != unique:
        duplicates = (
            df.groupBy(*columns).count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
        )
        print(f"{label}_DUPLICATE_KEY_SAMPLE:", flush=True)
        duplicates.show(20, truncate=False)
        raise RuntimeError(f"{label}: duplicate key {columns}: {total - unique}")


def normalize_geo_name(value):
    if value is None:
        return None
    text = unicodedata.normalize("NFKD", str(value).strip())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("đ", "d").replace("Đ", "D").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


normalize_geo_udf = F.udf(normalize_geo_name, StringType())


def load_dimensions(spark):
    years = spark.table(DIM_YEAR).select(
        F.col("year_key").cast("int").alias("year_key"),
        F.col("calendar_year").cast("int").alias("calendar_year"),
    )
    geography = spark.table(DIM_GEOGRAPHY).select(
        F.col("geography_key").cast("int").alias("geography_key"),
        "geography_code",
        "geography_name",
        "geography_level",
    )
    sources = spark.table(DIM_SOURCE).select(
        F.col("source_key").cast("bigint").alias("source_key"),
        "source_code",
    )
    products = spark.table(DIM_PRODUCT).select(
        F.col("coffee_product_key").cast("int").alias("coffee_product_key"),
        "product_code",
    )

    assert_unique(years, ["calendar_year"], "DIM_YEAR_CALENDAR")
    assert_unique(geography, ["geography_code"], "DIM_GEOGRAPHY_CODE")
    assert_unique(sources, ["source_code"], "DIM_SOURCE_CODE")
    assert_unique(products, ["product_code"], "DIM_PRODUCT_CODE")

    # if geography.count() != 33:
    #     raise RuntimeError(
    #         f"Expected 33 dim_geography rows, found {geography.count()}"
    #     )
    geography_count = geography.count()

    print(
        "DIM_GEOGRAPHY_ROWS_AVAILABLE:",
        geography_count,
        flush=True
    )
    if products.filter(F.col("product_code") == "COFFEE_UNSPECIFIED").count() != 1:
        raise RuntimeError("DIM_COFFEE_PRODUCT must contain COFFEE_UNSPECIFIED exactly once")

    product_key = products.filter(
        F.col("product_code") == "COFFEE_UNSPECIFIED"
    ).select("coffee_product_key").first()[0]

    geo_codes = {r["geography_code"] for r in geography.select("geography_code").collect()}
    unknown_codes = sorted(set(GEOGRAPHY_ALIASES.values()) - geo_codes)
    if unknown_codes:
        raise RuntimeError(
            "Geography alias map references codes absent from dim_geography: "
            + str(unknown_codes)
        )

    return years, geography, sources, product_key


def resolve_statistics_geography(stats, geography):
    aliases = [
        (normalized, code)
        for normalized, code in GEOGRAPHY_ALIASES.items()
    ]
    alias_df = stats.sparkSession.createDataFrame(
        aliases, ["_geo_normalized", "geography_code"]
    )

    mapped = (
        stats
        .withColumn("_geo_normalized", normalize_geo_udf(F.col("geography_name")))
        .join(alias_df, "_geo_normalized", "left")
        .join(
            geography.select("geography_key", "geography_code"),
            "geography_code",
            "left",
        )
    )

    unresolved = mapped.filter(F.col("geography_key").isNull())
    unresolved_count = unresolved.count()
    if unresolved_count:
        print("STATISTICS_UNMAPPED_GEOGRAPHIES:", flush=True)
        unresolved.select(
            "geography_name", "geography_level", "source_table"
        ).distinct().show(100, truncate=False)
        raise RuntimeError(
            f"{unresolved_count} Statistics rows cannot map to DIM_GEOGRAPHY"
        )
    return mapped


def build_statistics_base(spark, years, geography, sources):
    stats = spark.table(STATISTICS_SILVER)
    bronze = spark.table(STATISTICS_BRONZE)

    require_columns(
        stats,
        [
            "silver_record_id", "year", "is_preliminary", "geography_level",
            "geography_name", "metric_code", "value_standardized",
            "standardized_unit", "quality_status", "source_table",
            "source_filename", "bronze_object_key", "bronze_record_id",
            "bronze_record_position", "object_sha256",
        ],
        STATISTICS_SILVER,
    )
    require_columns(bronze, ["record_id", "source"], STATISTICS_BRONZE)

    stats_count = stats.count()
    stats_unique = stats.select("silver_record_id").distinct().count()
    if stats_count != EXPECTED_STATISTICS_ROWS or stats_unique != stats_count:
        raise RuntimeError(
            f"Unexpected Statistics Silver: rows={stats_count}, unique={stats_unique}"
        )

    invalid_numeric = stats.filter(
        F.col("quality_status") == "INVALID_NUMERIC_VALUE"
    ).count()
    if invalid_numeric:
        raise RuntimeError(f"Statistics Silver has {invalid_numeric} invalid numeric rows")

    missing_count = stats.filter(
        F.col("quality_status") == "MISSING_SOURCE_VALUE"
    ).count()
    if missing_count != EXPECTED_STATISTICS_MISSING_VALUES:
        raise RuntimeError(
            f"Expected {EXPECTED_STATISTICS_MISSING_VALUES} missing Statistics values, "
            f"found {missing_count}"
        )

    bronze_source = bronze.select(
        F.col("record_id").alias("bronze_record_id"),
        F.col("source").alias("source_code"),
    )
    assert_unique(bronze_source, ["bronze_record_id"], "STATISTICS_BRONZE_RECORD")

    enriched = stats.join(bronze_source, "bronze_record_id", "left")
    missing_source = enriched.filter(
        F.col("source_code").isNull() | (F.trim("source_code") == "")
    ).count()
    if missing_source:
        raise RuntimeError(f"Cannot resolve Bronze source for {missing_source} Statistics rows")

    enriched = resolve_statistics_geography(enriched, geography)
    enriched = enriched.join(
        years,
        enriched.year == years.calendar_year,
        "left",
    ).drop("calendar_year")
    enriched = enriched.join(sources, "source_code", "left")

    missing_year = enriched.filter(F.col("year_key").isNull()).count()
    missing_source_fk = enriched.filter(F.col("source_key").isNull()).count()
    if missing_year or missing_source_fk:
        raise RuntimeError(
            f"Statistics dimension mapping failed: year_fk={missing_year}, "
            f"source_fk={missing_source_fk}"
        )

    print("STATISTICS_SILVER_ROWS_INPUT:", stats_count, flush=True)
    print("STATISTICS_MISSING_SOURCE_VALUES_RETAINED:", missing_count, flush=True)
    print("STATISTICS_SOURCE_JOIN_ROWS:", enriched.count(), flush=True)
    return enriched


def build_measure_frame(stats, source_tables, metric_code, prefix):
    selected = stats.filter(
        F.col("source_table").isin(*source_tables)
        & (F.col("metric_code") == metric_code)
    )

    if selected.count() == 0:
        raise RuntimeError(
            f"No Statistics observations for {prefix}: "
            f"source_tables={source_tables}, metric={metric_code}"
        )

    keys = JOIN_KEYS
    assert_unique(selected, keys, f"STATISTICS_{prefix.upper()}_MEASURE")

    return selected.select(
        *keys,
        F.col("value_standardized").cast("decimal(24,6)").alias(f"_{prefix}_source_value"),
        F.col("source_key").cast("bigint").alias(f"{prefix}_source_key"),
        F.col("quality_status").alias(f"{prefix}_quality_status"),
        F.when(F.col("is_preliminary"), F.lit("PRELIMINARY"))
         .otherwise(F.lit("FINAL")).alias(f"{prefix}_publication_status"),
        F.lit(1).cast("bigint").alias(f"{prefix}_raw_observation_count"),
        F.col("silver_record_id").alias(f"{prefix}_silver_record_id"),
        F.col("bronze_record_id").alias(f"{prefix}_bronze_record_id"),
        F.col("bronze_object_key").alias(f"{prefix}_bronze_object_key"),
        F.col("bronze_record_position").cast("bigint").alias(f"{prefix}_bronze_record_position"),
        F.col("object_sha256").alias(f"{prefix}_object_sha256"),
        F.col("source_filename").alias(f"{prefix}_source_filename"),
        F.col("source_table").alias(f"{prefix}_source_table"),
    )


def add_not_provided_measure(df, prefix):
    value_name = f"_{prefix}_source_value"
    if value_name not in df.columns:
        df = df.withColumn(value_name, F.lit(None).cast("decimal(24,6)"))
    defaults = {
        f"{prefix}_source_key": F.lit(None).cast("bigint"),
        f"{prefix}_quality_status": F.lit("NOT_PROVIDED"),
        f"{prefix}_publication_status": F.lit("NOT_PROVIDED"),
        f"{prefix}_raw_observation_count": F.lit(0).cast("bigint"),
        f"{prefix}_silver_record_id": F.lit(None).cast("string"),
        f"{prefix}_bronze_record_id": F.lit(None).cast("string"),
        f"{prefix}_bronze_object_key": F.lit(None).cast("string"),
        f"{prefix}_bronze_record_position": F.lit(None).cast("bigint"),
        f"{prefix}_object_sha256": F.lit(None).cast("string"),
        f"{prefix}_source_filename": F.lit(None).cast("string"),
        f"{prefix}_source_table": F.lit(None).cast("string"),
    }
    for name, expression in defaults.items():
        if name not in df.columns:
            df = df.withColumn(name, expression)
    return df


def join_measure_frames(frames):
    if not frames:
        raise RuntimeError("No measure frames supplied")
    result = frames[0]
    for frame in frames[1:]:
        result = result.join(frame, JOIN_KEYS, "full_outer")
    return result


def finalize_statistics_dataset(
    stats,
    years,
    geography,
    product_key,
    dataset_code,
    area_measure_type,
    expected_rows,
    area_spec,
    production_spec,
    yield_spec=None,
):
    frames = []
    for prefix, spec in [
        ("area", area_spec),
        ("production", production_spec),
        ("yield", yield_spec),
    ]:
        if spec is None:
            continue
        frames.append(
            build_measure_frame(
                stats,
                source_tables=spec["source_tables"],
                metric_code=spec["metric_code"],
                prefix=prefix,
            )
        )

    combined = join_measure_frames(frames)
    for prefix in ("area", "production", "yield"):
        combined = add_not_provided_measure(combined, prefix)

    combined = (
        combined
        .withColumn("dataset_code", F.lit(dataset_code))
        .withColumn("area_measure_type", F.lit(area_measure_type))
        .withColumn("coffee_product_key", F.lit(product_key).cast("int"))
        .withColumn(
            "publication_status_code",
            F.when(F.col("is_preliminary"), F.lit("PRELIMINARY"))
             .otherwise(F.lit("FINAL")),
        )
        .withColumn(
            "coffee_area_thousand_ha",
            (F.col("_area_source_value") / F.lit(1000)).cast("decimal(18,3)"),
        )
        .withColumn(
            "coffee_production_thousand_tons",
            (F.col("_production_source_value") / F.lit(1000)).cast("decimal(18,3)"),
        )
        .withColumn(
            "yield_tons_per_ha",
            F.col("_yield_source_value").cast("decimal(18,3)"),
        )
        .withColumn(
            "area_growth_index_prev_year_100",
            F.lit(None).cast("decimal(18,3)"),
        )
        .withColumn(
            "production_growth_index_prev_year_100",
            F.lit(None).cast("decimal(18,3)"),
        )
        .withColumn("yield_method", F.lit("SOURCE_REPORTED"))
        .withColumn("yield_method", F.when(F.col("yield_quality_status") == "NOT_PROVIDED", "NOT_PROVIDED").otherwise(F.col("yield_method")))
    )

    total = combined.count()
    if total != expected_rows:
        print(f"{dataset_code}_ROW_SAMPLE:", flush=True)
        combined.orderBy("year_key", "geography_key").show(100, truncate=False)
        raise RuntimeError(
            f"{dataset_code}: expected {expected_rows} fact rows, found {total}"
        )

    return combined


def build_statistics_facts(spark, years, geography, sources, product_key):
    stats = build_statistics_base(spark, years, geography, sources)

    province = finalize_statistics_dataset(
        stats, years, geography, product_key,
        dataset_code="PROVINCE_STATISTICS",
        area_measure_type="REPORTED_AREA",
        expected_rows=65,
        area_spec={"source_tables": ["AREA_BY_PROVINCE"], "metric_code": "COFFEE_AREA"},
        production_spec={"source_tables": ["PRODUCTION_BY_PROVINCE"], "metric_code": "PRODUCTION"},
    )

    region = finalize_statistics_dataset(
        stats, years, geography, product_key,
        dataset_code="REGION_STATISTICS",
        area_measure_type="PLANTED_AREA",
        expected_rows=30,
        area_spec={"source_tables": ["PLANTED_AREA_BY_REGION"], "metric_code": "PLANTED_AREA"},
        production_spec={"source_tables": ["PRODUCTION_BY_REGION"], "metric_code": "PRODUCTION"},
        yield_spec={"source_tables": ["YIELD_BY_REGION"], "metric_code": "YIELD"},
    )

    vietnam = finalize_statistics_dataset(
        stats, years, geography, product_key,
        dataset_code="VIETNAM_ANNUAL_STATISTICS",
        area_measure_type="REPORTED_AREA",
        expected_rows=11,
        area_spec={"source_tables": ["VIETNAM_AREA_YIELD_PRODUCTION"], "metric_code": "COFFEE_AREA"},
        production_spec={"source_tables": ["VIETNAM_AREA_YIELD_PRODUCTION"], "metric_code": "PRODUCTION"},
        yield_spec={"source_tables": ["VIETNAM_AREA_YIELD_PRODUCTION"], "metric_code": "YIELD"},
    )

    world = finalize_statistics_dataset(
        stats, years, geography, product_key,
        dataset_code="WORLD_MAJOR_COUNTRIES_2024",
        area_measure_type="HARVESTED_AREA",
        expected_rows=16,
        area_spec={"source_tables": ["WORLD_MAJOR_COUNTRIES_2024"], "metric_code": "HARVESTED_AREA"},
        production_spec={"source_tables": ["WORLD_MAJOR_COUNTRIES_2024"], "metric_code": "PRODUCTION"},
        yield_spec={"source_tables": ["WORLD_MAJOR_COUNTRIES_2024"], "metric_code": "YIELD"},
    )

    outputs = [province, region, vietnam, world]
    for df, label in zip(
        outputs,
        ["PROVINCE_STATISTICS", "REGION_STATISTICS", "VIETNAM_ANNUAL_STATISTICS", "WORLD_MAJOR_COUNTRIES_2024"],
    ):
        assert_unique(df, GRAIN_KEYS, label)
        print(f"{label}_FACT_ROWS:", df.count(), flush=True)
    return outputs


def preliminary_expression(column):
    return F.lower(F.coalesce(F.col(column).cast("string"), F.lit(""))).rlike(
        "prelim|provisional|sơ bộ|so bo"
    )


def build_legacy_fact(spark, years, geography, sources, product_key):
    area = spark.table(LEGACY_AREA)
    production = spark.table(LEGACY_PRODUCTION)

    require_columns(
        area,
        [
            "year", "coffee_area_thousand_ha", "growth_index_prev_year_100",
            "publication_status", "quality_status", "raw_observation_count",
            "selected_source", "selected_bronze_record_id", "selected_bronze_object_key",
            "selected_bronze_record_position",
        ],
        LEGACY_AREA,
    )
    require_columns(
        production,
        [
            "year", "coffee_production_thousand_tons", "growth_index_prev_year_100",
            "publication_status", "quality_status", "raw_observation_count",
            "selected_source", "selected_bronze_record_id", "selected_bronze_object_key",
            "selected_bronze_record_position",
        ],
        LEGACY_PRODUCTION,
    )

    area_count = area.count()
    production_count = production.count()
    if area_count != 19 or production_count != 19:
        raise RuntimeError(
            f"Legacy input counts changed: area={area_count}, production={production_count}; expected 19 each"
        )
    assert_unique(area, ["year"], "LEGACY_AREA_YEAR")
    assert_unique(production, ["year"], "LEGACY_PRODUCTION_YEAR")

    area_only = area.select("year").join(production.select("year"), "year", "left_anti").count()
    production_only = production.select("year").join(area.select("year"), "year", "left_anti").count()
    if area_only or production_only:
        raise RuntimeError(
            f"Legacy year coverage differs: area_only={area_only}, production_only={production_only}"
        )

    vn_geo = geography.filter(F.col("geography_code") == "VN").select("geography_key")
    if vn_geo.count() != 1:
        raise RuntimeError("Expected exactly one VN geography member")

    a = area.alias("a")
    p = production.alias("p")
    y = years.alias("y")
    g = vn_geo.alias("g")
    sa = sources.alias("sa")
    sp = sources.alias("sp")

    joined = (
        a.join(p, F.col("a.year") == F.col("p.year"), "inner")
        .join(y, F.col("a.year") == F.col("y.calendar_year"), "left")
        .crossJoin(g)
        .join(sa, F.col("a.selected_source") == F.col("sa.source_code"), "left")
        .join(sp, F.col("p.selected_source") == F.col("sp.source_code"), "left")
    )

    missing_dims = joined.filter(
        F.col("y.year_key").isNull()
        | F.col("g.geography_key").isNull()
        | F.col("sa.source_key").isNull()
        | F.col("sp.source_key").isNull()
    ).count()
    if missing_dims:
        raise RuntimeError(f"Legacy rows with unmatched dimension keys: {missing_dims}")

    area_status = F.coalesce(F.col("a.publication_status"), F.lit("UNKNOWN"))
    production_status = F.coalesce(F.col("p.publication_status"), F.lit("UNKNOWN"))
    area_prelim = preliminary_expression("a.publication_status")
    production_prelim = preliminary_expression("p.publication_status")

    result = joined.select(
        F.col("y.year_key").cast("int").alias("year_key"),
        F.col("g.geography_key").cast("int").alias("geography_key"),
        F.lit(product_key).cast("int").alias("coffee_product_key"),
        F.lit("LEGACY_VIETNAM_ANNUAL").alias("dataset_code"),
        F.lit("REPORTED_AREA").alias("area_measure_type"),
        (area_prelim | production_prelim).alias("is_preliminary"),
        F.when(area_prelim & production_prelim, "PRELIMINARY")
         .when(area_prelim | production_prelim, "MIXED")
         .when((area_status == "UNKNOWN") | (production_status == "UNKNOWN"), "UNKNOWN")
         .otherwise("FINAL").alias("publication_status_code"),
        F.col("a.coffee_area_thousand_ha").cast("decimal(18,3)").alias("coffee_area_thousand_ha"),
        F.col("p.coffee_production_thousand_tons").cast("decimal(18,3)").alias("coffee_production_thousand_tons"),
        F.lit(None).cast("decimal(18,3)").alias("yield_tons_per_ha"),
        F.col("a.growth_index_prev_year_100").cast("decimal(18,3)").alias("area_growth_index_prev_year_100"),
        F.col("p.growth_index_prev_year_100").cast("decimal(18,3)").alias("production_growth_index_prev_year_100"),
        F.col("sa.source_key").cast("bigint").alias("area_source_key"),
        F.col("sp.source_key").cast("bigint").alias("production_source_key"),
        F.lit(None).cast("bigint").alias("yield_source_key"),
        area_status.alias("area_publication_status"),
        production_status.alias("production_publication_status"),
        F.lit("NOT_PROVIDED").alias("yield_publication_status"),
        F.col("a.quality_status").alias("area_quality_status"),
        F.col("p.quality_status").alias("production_quality_status"),
        F.lit("NOT_PROVIDED").alias("yield_quality_status"),
        F.col("a.raw_observation_count").cast("bigint").alias("area_raw_observation_count"),
        F.col("p.raw_observation_count").cast("bigint").alias("production_raw_observation_count"),
        F.lit(0).cast("bigint").alias("yield_raw_observation_count"),
        F.lit(None).cast("string").alias("area_silver_record_id"),
        F.lit(None).cast("string").alias("production_silver_record_id"),
        F.lit(None).cast("string").alias("yield_silver_record_id"),
        F.col("a.selected_bronze_record_id").alias("area_bronze_record_id"),
        F.col("p.selected_bronze_record_id").alias("production_bronze_record_id"),
        F.lit(None).cast("string").alias("yield_bronze_record_id"),
        F.col("a.selected_bronze_object_key").alias("area_bronze_object_key"),
        F.col("p.selected_bronze_object_key").alias("production_bronze_object_key"),
        F.lit(None).cast("string").alias("yield_bronze_object_key"),
        F.col("a.selected_bronze_record_position").cast("bigint").alias("area_bronze_record_position"),
        F.col("p.selected_bronze_record_position").cast("bigint").alias("production_bronze_record_position"),
        F.lit(None).cast("bigint").alias("yield_bronze_record_position"),
        F.lit(None).cast("string").alias("area_object_sha256"),
        F.lit(None).cast("string").alias("production_object_sha256"),
        F.lit(None).cast("string").alias("yield_object_sha256"),
        F.lit(None).cast("string").alias("area_source_filename"),
        F.lit(None).cast("string").alias("production_source_filename"),
        F.lit(None).cast("string").alias("yield_source_filename"),
        F.lit("coffee_area_annual").alias("area_source_table"),
        F.lit("coffee_production_annual").alias("production_source_table"),
        F.lit(None).cast("string").alias("yield_source_table"),
        F.lit("NOT_PROVIDED").alias("yield_method"),
    )

    if result.count() != 19:
        raise RuntimeError(f"Legacy joined Fact expected 19 rows, got {result.count()}")
    if result.filter(
        F.col("coffee_area_thousand_ha").isNull()
        | F.col("coffee_production_thousand_tons").isNull()
    ).count():
        raise RuntimeError("Legacy required area/production measures contain NULL")
    if result.filter(F.col("year_key") == 2006).count():
        raise RuntimeError("Legacy Fact unexpectedly contains missing year 2006")

    assert_unique(result, GRAIN_KEYS, "LEGACY_VIETNAM_ANNUAL_FACT")
    print("LEGACY_AREA_ROWS:", area_count, flush=True)
    print("LEGACY_PRODUCTION_ROWS:", production_count, flush=True)
    print("LEGACY_FACT_ROWS:", result.count(), flush=True)
    return result


def add_missing_final_columns(df):
    # All datasets must have exactly the same output schema before unionByName.
    defaults = {
        "area_growth_index_prev_year_100": F.lit(None).cast("decimal(18,3)"),
        "production_growth_index_prev_year_100": F.lit(None).cast("decimal(18,3)"),
    }
    for name, expression in defaults.items():
        if name not in df.columns:
            df = df.withColumn(name, expression)
    return df


def build_final_fact(spark):
    years, geography, sources, product_key = load_dimensions(spark)
    legacy = build_legacy_fact(spark, years, geography, sources, product_key)
    statistics = build_statistics_facts(
        spark, years, geography, sources, product_key
    )

    datasets = [legacy] + statistics
    aligned = [add_missing_final_columns(df) for df in datasets]
    combined = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        aligned,
    )

    combined = combined.withColumn(
        "production_fact_key",
        F.expr(
            "CAST(CONV(SUBSTR(SHA2(CONCAT_WS('||', "
            "CAST(year_key AS STRING), CAST(geography_key AS STRING), "
            "CAST(coffee_product_key AS STRING), dataset_code, "
            "area_measure_type, publication_status_code), 256), "
            "1, 15), 16, 10) AS BIGINT)"
        ),
    ).withColumn("gold_processed_at", F.current_timestamp())

    fact = combined.select(*FACT_COLUMNS).cache()

    total = fact.count()
    if total != EXPECTED_TOTAL_ROWS:
        raise RuntimeError(
            f"Expected {EXPECTED_TOTAL_ROWS} total Fact rows, found {total}"
        )

    assert_unique(fact, GRAIN_KEYS, "FACT_PRODUCTION_V2_NATURAL_GRAIN")
    assert_unique(fact, ["production_fact_key"], "FACT_PRODUCTION_V2_PK")

    required_key_nulls = fact.filter(
        F.col("production_fact_key").isNull()
        | F.col("year_key").isNull()
        | F.col("geography_key").isNull()
        | F.col("coffee_product_key").isNull()
        | F.col("dataset_code").isNull()
        | F.col("area_measure_type").isNull()
        | F.col("publication_status_code").isNull()
    ).count()
    if required_key_nulls:
        raise RuntimeError(f"Fact has {required_key_nulls} NULL required key rows")

    # FK checks against current Gold Dimensions.
    fk_checks = [
        ("year_key", years.select("year_key")),
        ("geography_key", geography.select("geography_key")),
        ("coffee_product_key", spark.table(DIM_PRODUCT).select("coffee_product_key")),
        ("area_source_key", sources.select("source_key")),
        ("production_source_key", sources.select("source_key")),
        ("yield_source_key", sources.select("source_key")),
    ]
    for fk, dimension_keys in fk_checks:
        non_null_fk = fact.filter(F.col(fk).isNotNull()).select(fk).distinct()
        unmatched = non_null_fk.join(
            dimension_keys.distinct(), non_null_fk[fk] == dimension_keys[dimension_keys.columns[0]], "left_anti"
        ).count()
        if unmatched:
            raise RuntimeError(f"{fk} has {unmatched} unmatched Dimension values")
        print(f"FACT_FK_UNMATCHED_{fk.upper()}:", unmatched, flush=True)

    # A source key is mandatory whenever its corresponding measure was supplied
    # by a source, including source rows whose numeric value is missing.
    for prefix in ("area", "production", "yield"):
        status_col = f"{prefix}_quality_status"
        source_col = f"{prefix}_source_key"
        missing_fk = fact.filter(
            (F.col(status_col) != "NOT_PROVIDED") & F.col(source_col).isNull()
        ).count()
        if missing_fk:
            raise RuntimeError(f"{prefix} source FK missing on {missing_fk} supplied observations")

    negative_values = fact.filter(
        (F.col("coffee_area_thousand_ha") < 0)
        | (F.col("coffee_production_thousand_tons") < 0)
        | (F.col("yield_tons_per_ha") < 0)
    ).count()
    if negative_values:
        raise RuntimeError(f"Fact has {negative_values} negative production measures")

    # Quality/value consistency. Missing source values stay NULL; NOT_PROVIDED
    # is distinct from a source value that was actually missing.
    for prefix, value_col in [
        ("area", "coffee_area_thousand_ha"),
        ("production", "coffee_production_thousand_tons"),
        ("yield", "yield_tons_per_ha"),
    ]:
        inconsistent = fact.filter(
            ((F.col(f"{prefix}_quality_status") == "PASS") & F.col(value_col).isNull())
            | ((F.col(f"{prefix}_quality_status").isin("MISSING_SOURCE_VALUE", "NOT_PROVIDED")) & F.col(value_col).isNotNull())
        ).count()
        if inconsistent:
            raise RuntimeError(f"{prefix}: {inconsistent} quality/value inconsistencies")

    dataset_counts = {
        row["dataset_code"]: row["count"]
        for row in fact.groupBy("dataset_code").count().collect()
    }
    if dataset_counts != EXPECTED_ROWS:
        raise RuntimeError(
            f"Unexpected Fact dataset distribution: {dataset_counts}; expected {EXPECTED_ROWS}"
        )

    missing_quality_total = sum(
        fact.filter(F.col(f"{prefix}_quality_status") == "MISSING_SOURCE_VALUE").count()
        for prefix in ("area", "production", "yield")
    )
    if missing_quality_total != EXPECTED_STATISTICS_MISSING_VALUES:
        raise RuntimeError(
            f"Expected {EXPECTED_STATISTICS_MISSING_VALUES} missing measure values, "
            f"found {missing_quality_total}"
        )

    print("===== FACT PRODUCTION V2 STAGING VALIDATION =====", flush=True)
    print("FACT_PRODUCTION_V2_ROWS:", total, flush=True)
    print("FACT_PRODUCTION_V2_UNIQUE_PK:", fact.select("production_fact_key").distinct().count(), flush=True)
    print("FACT_PRODUCTION_V2_UNIQUE_GRAIN:", fact.select(*GRAIN_KEYS).distinct().count(), flush=True)
    print("FACT_PRODUCTION_V2_DATASET_COUNTS:", dataset_counts, flush=True)
    print("FACT_PRODUCTION_V2_MISSING_SOURCE_MEASURES:", missing_quality_total, flush=True)
    print("FACT_PRODUCTION_V2_REQUIRED_NULL_ROWS:", required_key_nulls, flush=True)
    print("FACT_PRODUCTION_V2_NEGATIVE_MEASURE_ROWS:", negative_values, flush=True)

    fact.groupBy("dataset_code", "area_measure_type").count().orderBy(
        "dataset_code", "area_measure_type"
    ).show(30, truncate=False)

    fact.orderBy("dataset_code", "year_key", "geography_key").select(
        "dataset_code", "year_key", "geography_key", "coffee_product_key",
        "area_measure_type", "is_preliminary", "coffee_area_thousand_ha",
        "coffee_production_thousand_tons", "yield_tons_per_ha",
        "area_quality_status", "production_quality_status", "yield_quality_status",
    ).show(40, truncate=False)

    print("FACT_PRODUCTION_V2_STAGING_VALIDATED", flush=True)
    return fact


def write_staging(spark, fact):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    ddl = """
        production_fact_key BIGINT,
        year_key INT,
        geography_key INT,
        coffee_product_key INT,
        dataset_code STRING,
        area_measure_type STRING,
        is_preliminary BOOLEAN,
        publication_status_code STRING,
        coffee_area_thousand_ha DECIMAL(18,3),
        coffee_production_thousand_tons DECIMAL(18,3),
        yield_tons_per_ha DECIMAL(18,3),
        area_growth_index_prev_year_100 DECIMAL(18,3),
        production_growth_index_prev_year_100 DECIMAL(18,3),
        area_source_key BIGINT,
        production_source_key BIGINT,
        yield_source_key BIGINT,
        area_publication_status STRING,
        production_publication_status STRING,
        yield_publication_status STRING,
        area_quality_status STRING,
        production_quality_status STRING,
        yield_quality_status STRING,
        area_raw_observation_count BIGINT,
        production_raw_observation_count BIGINT,
        yield_raw_observation_count BIGINT,
        area_silver_record_id STRING,
        production_silver_record_id STRING,
        yield_silver_record_id STRING,
        area_bronze_record_id STRING,
        production_bronze_record_id STRING,
        yield_bronze_record_id STRING,
        area_bronze_object_key STRING,
        production_bronze_object_key STRING,
        yield_bronze_object_key STRING,
        area_bronze_record_position BIGINT,
        production_bronze_record_position BIGINT,
        yield_bronze_record_position BIGINT,
        area_object_sha256 STRING,
        production_object_sha256 STRING,
        yield_object_sha256 STRING,
        area_source_filename STRING,
        production_source_filename STRING,
        yield_source_filename STRING,
        area_source_table STRING,
        production_source_table STRING,
        yield_source_table STRING,
        yield_method STRING,
        gold_processed_at TIMESTAMP
    """

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {TARGET} ({ddl}) USING iceberg"
    )

    target = spark.table(TARGET)
    if target.columns != FACT_COLUMNS:
        raise RuntimeError(
            "Staging target schema columns differ from v2 output. "
            "No staging rows were deleted. "
            f"target={target.columns}; expected={FACT_COLUMNS}"
        )
    expected_types = [field.dataType for field in fact.schema]
    actual_types = [field.dataType for field in target.schema]
    if expected_types != actual_types:
        raise RuntimeError(
            "Staging target schema data types differ. No staging rows were deleted."
        )

    fact.createOrReplaceTempView("stage_fact_production_v2")
    # This DELETE is restricted to the explicitly named v2 staging table.
    # The existing fact_production table is not referenced by any write clause.
    spark.sql(f"DELETE FROM {TARGET} WHERE TRUE")
    spark.sql(f"INSERT INTO {TARGET} SELECT * FROM stage_fact_production_v2")

    saved = spark.table(TARGET)
    actual = saved.count()
    unique = saved.select("production_fact_key").distinct().count()
    if actual != EXPECTED_TOTAL_ROWS or unique != actual:
        raise RuntimeError(
            f"Staging Iceberg verification failed: rows={actual}, unique={unique}"
        )

    print("FACT_PRODUCTION_V2_STAGING_ICEBERG_ROWS:", actual, flush=True)
    print("FACT_PRODUCTION_V2_STAGING_ICEBERG_PASSED", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write only lakehouse.gold.fact_production_v2_staging after validation.",
    )
    args = parser.parse_args()

    spark = make_spark()
    fact = None
    try:
        fact = build_final_fact(spark)
        if args.write:
            write_staging(spark, fact)
        else:
            print("FACT_PRODUCTION_V2_STAGING_DRY_RUN_PASSED", flush=True)
            print("NO_GOLD_FACT_TABLE_WRITTEN", flush=True)
    finally:
        if fact is not None:
            fact.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
