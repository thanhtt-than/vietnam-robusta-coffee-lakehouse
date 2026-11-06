"""TLCN Silver Iceberg: annual coffee area, production and long-form trade.

Read-only by default. Run with --write to MERGE into Nessie Iceberg.
Never changes Bronze. Keep missing 2006 as missing; keep World separate.
"""
import argparse
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import DecimalType

CATALOG = "lakehouse"
BRONZE = "lakehouse.bronze"
SILVER = "lakehouse.silver"


def make_spark():
    spark = (
        SparkSession.builder.appName("TLCN-Silver-Historical-Stats")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/iceberg/main/")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def text_value(j, field):
    return F.trim(j.getItem(field))


def decimal_value(expression):
    # Historical sources use plain decimal or comma as thousands separator.
    return F.regexp_replace(F.trim(expression), ",", "").cast(DecimalType(20, 3))


def bronze_rows(spark, name):
    df = spark.table(f"{BRONZE}.{name}")
    return df.withColumn("j", F.from_json(F.col("raw_record_json"), "MAP<STRING,STRING>"))


def canonicalize(df, keys, comparison_columns):
    """Keep historical_csv first, then deterministic lineage; surface conflicts."""
    value_parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in comparison_columns]
    fingerprint = F.sha2(F.to_json(F.array(*value_parts)), 256)
    window = Window.partitionBy(*keys).orderBy(
        F.when(F.col("source") == "historical_csv", 0).otherwise(1),
        F.col("object_key").asc(),
        F.col("record_position").asc(),
        F.col("record_id").asc(),
    )
    prepared = df.withColumn("_fingerprint", fingerprint)
    stats = prepared.groupBy(*keys).agg(
        F.count("*").alias("raw_observation_count"),
        F.countDistinct("_fingerprint").alias("distinct_values_count"),
    )
    selected = prepared.withColumn("_rownum", F.row_number().over(window))
    return (
        selected.filter(F.col("_rownum") == 1)
        .drop("_rownum", "_fingerprint")
        .join(stats, keys, "inner")
    )


def annual(spark, domain):
    assert domain in {"area", "production"}
    raw = bronze_rows(spark, f"coffee_{domain}_raw")
    source_count = raw.count()
    metric_name = "coffee_area_thousand_ha" if domain == "area" else "coffee_production_thousand_tons"
    growth_name = f"coffee_{domain}_growth_index_prev_year_100"
    df = raw.select(
        F.col("record_id").alias("selected_bronze_record_id"),
        F.col("object_key").alias("selected_bronze_object_key"),
        "record_position", "source", "object_key", "record_id",
        text_value(F.col("j"), "year").cast("int").alias("year"),
        decimal_value(text_value(F.col("j"), metric_name)).alias(metric_name),
        decimal_value(text_value(F.col("j"), growth_name)).alias("growth_index_prev_year_100"),
        text_value(F.col("j"), "status").alias("publication_status"),
    )
    invalid_years = df.filter(F.col("year").isNull() | ~F.col("year").between(1900, 2100)).count()
    if invalid_years:
        raise RuntimeError(f"{domain}: invalid year rows={invalid_years}; no writes permitted")
    result = canonicalize(df, ["year"], [metric_name, "growth_index_prev_year_100", "publication_status"])
    result = result.select(
        "year", metric_name, "growth_index_prev_year_100", "publication_status",
        "raw_observation_count", "distinct_values_count",
        F.when(F.col(metric_name).isNull() | (F.col(metric_name) < 0), "INVALID_METRIC")
        .when(F.col("distinct_values_count") > 1, "SOURCE_CONFLICT_REVIEW")
        .otherwise("PASS").alias("quality_status"),
        F.col("source").alias("selected_source"), "selected_bronze_record_id",
        "selected_bronze_object_key", F.col("record_position").alias("selected_bronze_record_position"),
        F.current_timestamp().alias("silver_processed_at"),
    ).cache()
    count = result.count()
    observed = set(x[0] for x in result.select("year").collect())
    missing = sorted(set(range(min(observed), max(observed) + 1)) - observed)
    print(f"{domain.upper()}_BRONZE_ROWS: {source_count}", flush=True)
    print(f"{domain.upper()}_SILVER_ROWS: {count}", flush=True)
    print(f"{domain.upper()}_MISSING_YEARS: {missing}", flush=True)
    result.groupBy("quality_status").count().show(truncate=False)
    result.orderBy("year").show(4, truncate=False)
    if count != 19 or 2006 not in missing:
        raise RuntimeError(f"{domain}: unexpected historical coverage; inspect before writing")
    return result, ["year"]


def trade(spark):
    raw = bronze_rows(spark, "coffee_trade_raw")
    source_count = raw.count()
    base = raw.select(
        "record_id", "object_key", "record_position", "source", "j",
        text_value(F.col("j"), "reporterCd").alias("reporter_code"),
        text_value(F.col("j"), "reporterLabel").alias("reporter_name"),
        text_value(F.col("j"), "partnerCd").alias("partner_code"),
        text_value(F.col("j"), "partnerLabel").alias("partner_name"),
        text_value(F.col("j"), "productCd").alias("product_code"),
        text_value(F.col("j"), "productLabel").alias("product_name"),
    )
    values = [
        F.struct(
            F.lit(year).alias("year"),
            text_value(F.col("j"), f"{year} (USD Thousand)").alias("raw_trade_value"),
        )
        for year in range(2001, 2025)
    ]
    df = base.withColumn("_annual", F.explode(F.array(*values))).select(
        "record_id", "object_key", "record_position", "source",
        "reporter_code", "reporter_name", "partner_code", "partner_name", "product_code", "product_name",
        F.col("_annual.year").alias("year"),
        F.col("_annual.raw_trade_value").alias("raw_trade_value"),
    ).withColumn("value_usd_thousand", decimal_value(F.col("raw_trade_value")))
    bad_keys = df.filter(
        F.col("reporter_code").isNull() | (F.col("reporter_code") == "") |
        F.col("partner_code").isNull() | (F.col("partner_code") == "") |
        F.col("product_code").isNull() | (F.col("product_code") == "")
    ).count()
    if bad_keys:
        raise RuntimeError(f"trade: {bad_keys} rows with missing business keys; no writes permitted")
    keys = ["reporter_code", "partner_code", "product_code", "year"]
    result = canonicalize(df, keys, ["raw_trade_value", "reporter_name", "partner_name", "product_name"])
    result = result.select(
        *keys, "reporter_name", "partner_name", "product_name", "value_usd_thousand",
        (F.upper(F.col("partner_name")) == "WORLD").__or__(F.col("partner_code") == "000").alias("is_world_aggregate"),
        "raw_observation_count", "distinct_values_count",
        F.when(F.col("raw_trade_value").isNull() | (F.col("raw_trade_value") == ""), "MISSING_VALUE")
        .when(F.col("value_usd_thousand").isNull() | (F.col("value_usd_thousand") < 0), "INVALID_VALUE")
        .when(F.col("distinct_values_count") > 1, "SOURCE_CONFLICT_REVIEW")
        .otherwise("PASS").alias("quality_status"),
        F.col("source").alias("selected_source"),
        F.col("record_id").alias("selected_bronze_record_id"),
        F.col("object_key").alias("selected_bronze_object_key"),
        F.col("record_position").alias("selected_bronze_record_position"),
        F.current_timestamp().alias("silver_processed_at"),
    ).cache()
    count = result.count()
    print(f"TRADE_BRONZE_ROWS: {source_count}", flush=True)
    print(f"TRADE_SILVER_ROWS: {count}", flush=True)
    print(f"TRADE_WORLD_ROWS: {result.filter('is_world_aggregate').count()}", flush=True)
    result.groupBy("quality_status").count().show(truncate=False)
    result.orderBy("year", "partner_code").show(5, truncate=False)
    if count != 162 * 24:
        raise RuntimeError(f"trade: expected 162 partners x 24 years = 3888, got {count}")
    return result, keys


def write_iceberg(spark, name, df, key):
    target = f"{SILVER}.{name}"
    view = f"stage_{name}"
    df.createOrReplaceTempView(view)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} USING iceberg AS SELECT * FROM {view} WHERE 1 = 0")
    join = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in key)
    # Only insert missing rows. This avoids overwriting existing approved Silver
    # rows or creating new Iceberg snapshots for identical reruns.
    spark.sql(f"MERGE INTO {target} t USING {view} s ON {join} WHEN NOT MATCHED THEN INSERT *")
    total = spark.table(target).count()
    unique = spark.table(target).select(*key).distinct().count()
    if unique != total:
        raise RuntimeError(f"{target}: duplicate Silver business keys: {unique}/{total}")
    expected = df.count()
    if total != expected:
        raise RuntimeError(f"{target}: total={total}, source={expected}; inspect preexisting records")
    print(f"{name.upper()}_ICEBERG_TOTAL: {total}", flush=True)
    print(f"{name.upper()}_ICEBERG_PASSED", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="Write / MERGE Silver Iceberg tables")
    args = parser.parse_args()
    spark = make_spark()
    try:
        outputs = [
            ("coffee_area_annual", *annual(spark, "area")),
            ("coffee_production_annual", *annual(spark, "production")),
            ("coffee_trade_annual", *trade(spark)),
        ]
        for name, df, key in outputs:
            if df.select(*key).distinct().count() != df.count():
                raise RuntimeError(f"{name}: duplicate keys remain")
        print("SILVER_HISTORICAL_DRY_RUN_PASSED", flush=True)
        if args.write:
            for name, df, key in outputs:
                write_iceberg(spark, name, df, key)
            print("SILVER_HISTORICAL_ICEBERG_PASSED", flush=True)
        else:
            print("NO_SILVER_TABLE_WRITTEN", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
