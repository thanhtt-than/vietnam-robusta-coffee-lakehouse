from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Integrated-Gold-Warehouse")
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

expected = {
    "dim_article": 1218,
    "dim_date": 9862,
    "dim_destination": 162,
    "dim_fuel_series": 58,
    "dim_geography": 5,
    "dim_hs_product": 1,
    "dim_news_topic": 9,
    "dim_price_series": 12,
    "dim_source": 14,
    "dim_weather_location": 12,
    "dim_year": 27,
    "fact_domestic_price": 9409,
    "fact_export": 3888,
    "fact_fuel_price": 1704,
    "fact_futures_price": 4755,
    "fact_news_topic": 1472,
    "fact_production": 19,
    "fact_weather_daily": 97620,
}

try:
    for name, expected_rows in expected.items():
        actual_rows = spark.table(f"lakehouse.gold.{name}").count()
        status = "PASS" if actual_rows == expected_rows else "FAIL"
        print(f"{name}: {actual_rows}/{expected_rows} [{status}]", flush=True)
        if actual_rows != expected_rows:
            raise RuntimeError(f"Row count mismatch: {name}")

    print("ALL_18_GOLD_WAREHOUSE_TABLES_VERIFIED", flush=True)
finally:
    spark.stop()
