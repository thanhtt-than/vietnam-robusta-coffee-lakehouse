"""Read-only audit of existing Gold Data Marts and supporting Fact tables.

Run from the project root with run_spark.ps1. This script does not write,
replace, drop, or mutate any Iceberg table.
"""
from pathlib import Path
import sys

from pyspark.sql import functions as F

# minio_direct.py is maintained under src/iceberg.
# Add that directory because this audit script runs from src/gold/marts.
ICEBERG_UTILS_DIR = Path(__file__).resolve().parents[2] / "iceberg"
if ICEBERG_UTILS_DIR.is_dir():
    sys.path.insert(0, str(ICEBERG_UTILS_DIR))

from minio_direct import spark_session

MARTS = [
    "mart_coffee_market_daily",
    "mart_coffee_price_trends",
    "mart_futures_domestic_spread",
    "mart_coffee_production_annual",
    "mart_coffee_export_annual",
    "mart_weather_price_impact",
    "mart_fuel_price_impact",
    "mart_market_news_analytics",
]

SUPPORTING_TABLES = [
    "fact_domestic_price",
    "fact_futures_price",
    "fact_weather_daily",
    "fact_fuel_price",
    "fact_exchange_rate",
    "fact_coffee_trade_monthly",
    "fact_global_coffee_exports",
    "fact_robusta_stock_port",
    "fact_robusta_stock_totals",
    "fact_psd",
    "fact_export",
    "fact_production",
    "fact_news_topic",
    "dim_date",
    "dim_geography",
    "dim_price_series",
    "dim_fuel_series",
    "dim_currency_pair",
    "dim_weather_location",
    "dim_port",
    "dim_destination",
    "dim_hs_product",
    "dim_coffee_product",
    "dim_year",
]

# Candidate keys describe the intended grain. A check runs only when all
# listed columns exist in the actual table schema.
GRAIN_KEYS = {
    "mart_coffee_market_daily": ["market_date", "province"],
    "mart_coffee_price_trends": ["market_date", "province", "price_series_key"],
    "mart_futures_domestic_spread": ["market_date", "province", "price_series_key"],
    "mart_coffee_production_annual": ["year", "province"],
    "mart_coffee_export_annual": ["export_year", "partner_name", "product_code", "is_world_aggregate"],
    "mart_weather_price_impact": ["market_date", "province", "price_series_key"],
    "mart_fuel_price_impact": ["market_date", "province", "price_series_key", "fuel_series_key"],
    "mart_market_news_analytics": ["market_date", "topic_key"],
}

DATE_COLUMNS = ["market_date", "trade_date", "effective_date", "export_year", "year"]
PRICE_COLUMNS = [
    "coffee_price_vnd_kg",
    "domestic_price_vnd_kg",
    "fuel_price_vnd_per_unit",
    "diesel_price_vnd_l",
    "futures_avg_last_price",
    "futures_last_price",
]


def qname(name):
    return f"lakehouse.gold.{name}"


def main():
    spark = spark_session("TLCN-Audit-Gold-Marts-ReadOnly")
    try:
        rows = spark.sql("SHOW TABLES IN lakehouse.gold").collect()
        existing = {r["tableName"].lower() for r in rows}
        print("AUDIT_READ_ONLY: true")
        print("GOLD_TABLE_COUNT:", len(existing))
        print("GOLD_TABLE_NAMES:", ",".join(sorted(existing)))

        print("\n========== EXISTING DATA MARTS ==========")
        for name in MARTS:
            if name.lower() not in existing:
                print(f"\nMART_MISSING: {name}")
                continue

            df = spark.table(qname(name))
            columns = [c.lower() for c in df.columns]
            print(f"\nMART: {name}")
            print("ROW_COUNT:", df.count())
            print("SCHEMA:", " | ".join(f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields))

            keys = GRAIN_KEYS.get(name, [])
            if keys and all(k in columns for k in keys):
                dup = (df.groupBy(*keys).count().filter(F.col("count") > 1).count())
                null_key_expr = None
                for k in keys:
                    e = F.col(k).isNull()
                    null_key_expr = e if null_key_expr is None else (null_key_expr | e)
                null_keys = df.filter(null_key_expr).count()
                print("GRAIN_KEY:", ",".join(keys))
                print("DUPLICATE_GRAIN_KEYS:", dup)
                print("NULL_GRAIN_KEYS:", null_keys)
            else:
                print("GRAIN_KEY_CHECK: NOT_RUN (candidate columns missing)")

            present_dates = [c for c in DATE_COLUMNS if c in columns]
            for c in present_dates[:1]:
                if c in ("export_year", "year"):
                    stats = df.agg(F.min(F.col(c).cast("string")).alias("min_value"),
                                   F.max(F.col(c).cast("string")).alias("max_value")).first()
                else:
                    stats = df.agg(F.min(c).alias("min_value"), F.max(c).alias("max_value")).first()
                print(f"DATE_RANGE_{c}:", stats["min_value"], "->", stats["max_value"])

            for c in PRICE_COLUMNS:
                if c in columns:
                    invalid = df.filter(F.col(c).isNotNull() & (F.col(c).cast("double") <= 0)).count()
                    nulls = df.filter(F.col(c).isNull()).count()
                    print(f"COLUMN_CHECK_{c}: nulls={nulls}, nonpositive={invalid}")

            # Print a small sample for manual semantic review, without collecting full data.
            df.show(3, truncate=False)

        print("\n========== SUPPORTING FACT / DIMENSION TABLES ==========")
        for name in SUPPORTING_TABLES:
            if name.lower() not in existing:
                print(f"SOURCE_TABLE_MISSING: {name}")
                continue
            df = spark.table(qname(name))
            print(f"SOURCE_TABLE: {name} | ROWS: {df.count()}")
            print("SOURCE_SCHEMA:", " | ".join(f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields))

        print("\nGOLD_MART_READ_ONLY_AUDIT_PASSED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
