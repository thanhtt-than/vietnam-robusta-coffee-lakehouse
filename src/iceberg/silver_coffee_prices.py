"""Silver Coffee Prices from existing Iceberg Bronze tables.

Default DRY RUN is read-only. --write explicitly publishes canonical source/day/province
observations to Iceberg, without modifying Bronze or MinIO raw objects.
Run: .\run_spark.ps1 -Script 'src/iceberg/silver_coffee_prices.py'
Optional write: --write (run_spark.ps1 may need argument forwarding).
"""
import sys
from pyspark.sql import SparkSession, functions as F, Window

CATALOG = "lakehouse"
TARGET = "lakehouse.silver.coffee_prices_daily"
COFFEE_RAW = "lakehouse.bronze.coffee_prices_raw"
DOMESTIC_RAW = "lakehouse.bronze.domestic_prices_raw"
VALID_REGIONS = ["DAK_LAK", "DAK_NONG", "GIA_LAI", "LAM_DONG"]


def field(name):
    return F.get_json_object(F.col("raw_record_json"), "$." + name)


def region_normalized(column):
    # Original region is retained as raw_record_json in Bronze.
    value = F.upper(F.trim(column))
    value = F.regexp_replace(value, r"[\s\-]+", "_")
    return (F.when(value.isin("DAK_LAK", "ĐẮK_LẮK", "ĐẮK_LAK", "DAKLAK"), "DAK_LAK")
            .when(value.isin("DAK_NONG", "ĐẮK_NÔNG", "ĐẮK_NONG", "DAKNONG"), "DAK_NONG")
            .when(value.isin("GIA_LAI", "GIALAI"), "GIA_LAI")
            .when(value.isin("LAM_DONG", "LÂM_ĐỒNG", "LAMDONG"), "LAM_DONG")
            .otherwise(value))


def build_inputs(spark):
    b = spark.table(COFFEE_RAW)
    # The BBW source may also return an FX series. Strict series allowlist protects Silver.
    bbw = b.select(
        "record_id", "object_key", "raw_record_json",
        F.lit("bbw_coffee").alias("source_family"),
        region_normalized(field("region")).alias("province"),
        F.to_date(F.substring(field("quote_date_vn"), 1, 10)).alias("quote_date"),
        field("price").alias("raw_price"),
        F.upper(F.trim(field("currency"))).alias("currency"),
        F.upper(F.trim(field("unit"))).alias("unit"),
        F.upper(F.trim(field("product"))).alias("product"),
        F.upper(F.trim(field("series_code"))).alias("series_code"),
        field("updated_at").alias("updated_at_raw"),
    )
    d = spark.table(DOMESTIC_RAW)
    e = d.filter(F.col("record_type") == "enriched_csv").select(
        "record_id", "object_key", "raw_record_json",
        F.lit("enriched_csv").alias("source_family"),
        region_normalized(field("province")).alias("province"),
        F.to_date(F.substring(field("date"), 1, 10)).alias("quote_date"),
        field("coffee_price_vnd_kg").alias("raw_price"),
        F.lit("VND").alias("currency"),
        F.lit("VND/KG").alias("unit"),
        F.when(F.lower(F.coalesce(field("commodity"), F.lit(""))).contains("coffee"),
               F.lit("COFFEE")).otherwise(F.lit("OTHER")).alias("product"),
        F.lit(None).cast("string").alias("series_code"),
        F.lit(None).cast("string").alias("updated_at_raw"),
    )
    a = d.filter(F.col("record_type") == "json_api_snapshot").select(
        "record_id", "object_key", "raw_record_json",
        F.lit("legacy_api_json").alias("source_family"),
        region_normalized(field("region")).alias("province"),
        F.to_date(F.substring(field("quote_date_vn"), 1, 10)).alias("quote_date"),
        field("price").alias("raw_price"),
        F.upper(F.trim(field("currency"))).alias("currency"),
        F.upper(F.trim(field("unit"))).alias("unit"),
        F.upper(F.trim(field("product"))).alias("product"),
        F.upper(F.trim(field("series_code"))).alias("series_code"),
        field("updated_at").alias("updated_at_raw"),
    )
    result = bbw.unionByName(e).unionByName(a)
    # Cast only plain decimal or integer strings; ambiguous locale formats are rejected.
    result = result.withColumn(
        "price_vnd_kg",
        F.when(F.col("raw_price").rlike(r"^\s*\d+(?:\.\d+)?\s*$"),
               F.col("raw_price").cast("decimal(18,2)"))
         .otherwise(F.lit(None).cast("decimal(18,2)"))
    )
    valid_series = (F.col("source_family") == "enriched_csv") | (
        F.col("series_code") == F.concat(F.lit("COFFEE/"), F.col("province"))
    )
    valid = (
        F.col("province").isin(VALID_REGIONS)
        & F.col("quote_date").isNotNull()
        & F.col("price_vnd_kg").isNotNull()
        & (F.col("price_vnd_kg") > 0)
        & (F.col("product") == "COFFEE")
        & (F.col("currency") == "VND")
        & (F.col("unit") == "VND/KG")
        & valid_series
    )
    return result.withColumn("is_comparable_coffee", valid)


def make_canonical(comparable):
    key = ["source_family", "province", "quote_date"]
    aggregate = comparable.groupBy(*key).agg(
        F.count("*").alias("raw_observation_count"),
        F.countDistinct("price_vnd_kg").alias("distinct_price_count"),
        F.min("price_vnd_kg").alias("min_price_vnd_kg"),
        F.max("price_vnd_kg").alias("max_price_vnd_kg"),
    )
    # Do not merge different sources, or hide within-source price conflicts.
    order = Window.partitionBy(*key).orderBy(
        F.col("updated_at_raw").desc_nulls_last(),
        F.col("object_key").asc(),
        F.col("record_id").asc(),
    )
    winner = (comparable.withColumn("_rank", F.row_number().over(order))
              .filter(F.col("_rank") == 1).drop("_rank"))
    out = winner.join(aggregate, key, "inner").select(
        "source_family", "province", "quote_date", "price_vnd_kg",
        F.when(F.col("distinct_price_count") > 1, F.lit("PRICE_CONFLICT_REVIEW"))
         .otherwise(F.lit("PASS")).alias("quality_status"),
        "raw_observation_count", "distinct_price_count",
        "min_price_vnd_kg", "max_price_vnd_kg",
        F.col("record_id").alias("selected_bronze_record_id"),
        F.col("object_key").alias("selected_bronze_object_key"),
        F.current_timestamp().alias("silver_processed_at"),
    )
    return out


def main():
    write = "--write" in sys.argv[1:]
    spark = (SparkSession.builder.appName("TLCN-Silver-Coffee-Prices")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/iceberg/main/")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = build_inputs(spark).cache()
        total = df.count()
        print("SOURCE_TOTAL:", total, flush=True)
        if total != 3525 + 13915:
            raise RuntimeError(f"Expected 17440 source rows, observed {total}; review new ingestion")
        df.groupBy("source_family", "is_comparable_coffee").count().orderBy(
            "source_family", "is_comparable_coffee").show(20, truncate=False)
        comparable = df.filter("is_comparable_coffee").cache()
        accepted = comparable.count()
        excluded = total - accepted
        canonical = make_canonical(comparable).cache()
        final_count = canonical.count()
        print("COMPARABLE_ROWS:", accepted, flush=True)
        print("EXCLUDED_NONCOMPARABLE_OR_INVALID:", excluded, flush=True)
        print("CANONICAL_SOURCE_PROVINCE_DAYS:", final_count, flush=True)
        canonical.groupBy("source_family", "quality_status").count().orderBy(
            "source_family", "quality_status").show(truncate=False)
        print("CONFLICT_EXAMPLES:", flush=True)
        canonical.filter("quality_status != 'PASS'").select(
            "source_family", "province", "quote_date", "raw_observation_count",
            "distinct_price_count", "min_price_vnd_kg", "max_price_vnd_kg").show(12, truncate=False)
        distinct_keys = canonical.select("source_family", "province", "quote_date").distinct().count()
        if distinct_keys != final_count or accepted < final_count or final_count == 0:
            raise RuntimeError("Silver row accounting or business-key uniqueness failed")
        if not write:
            print("SILVER_COFFEE_DRY_RUN_PASSED", flush=True)
            print("NO_SILVER_TABLE_WRITTEN", flush=True)
            return
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (
            source_family STRING, province STRING, quote_date DATE,
            price_vnd_kg DECIMAL(18,2), quality_status STRING,
            raw_observation_count BIGINT, distinct_price_count BIGINT,
            min_price_vnd_kg DECIMAL(18,2), max_price_vnd_kg DECIMAL(18,2),
            selected_bronze_record_id STRING, selected_bronze_object_key STRING,
            silver_processed_at TIMESTAMP
        ) USING iceberg""")
        canonical.createOrReplaceTempView("coffee_prices_canonical_stage")
        # Stable replacement of the entire canonical representation in one SQL statement.
        # Existing Bronze remains untouched. Source scope is three known families only.
        spark.sql(f"""MERGE INTO {TARGET} t USING coffee_prices_canonical_stage s
            ON t.source_family = s.source_family AND t.province = s.province AND t.quote_date = s.quote_date
            WHEN MATCHED THEN UPDATE SET
                t.price_vnd_kg=s.price_vnd_kg, t.quality_status=s.quality_status,
                t.raw_observation_count=s.raw_observation_count,
                t.distinct_price_count=s.distinct_price_count,
                t.min_price_vnd_kg=s.min_price_vnd_kg,
                t.max_price_vnd_kg=s.max_price_vnd_kg,
                t.selected_bronze_record_id=s.selected_bronze_record_id,
                t.selected_bronze_object_key=s.selected_bronze_object_key
            WHEN NOT MATCHED THEN INSERT *""")
        actual = spark.table(TARGET).count()
        print("SILVER_COFFEE_TOTAL:", actual, flush=True)
        if actual != final_count:
            raise RuntimeError("Silver table contains unexpected extra rows; review without deleting")
        print("SILVER_COFFEE_ICEBERG_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
