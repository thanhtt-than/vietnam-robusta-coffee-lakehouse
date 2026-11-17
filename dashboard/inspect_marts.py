from pyspark.sql import SparkSession

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

spark = SparkSession.builder.appName(
    "Inspect-Dashboard-Marts"
).getOrCreate()

try:
    for name in MARTS:
        path = f"/workspace/dashboard/data/{name}"
        df = spark.read.parquet(path)

        print(f"\n=== {name} ===", flush=True)

        for field in df.schema.fields:
            print(
                f"{field.name}: {field.dataType.simpleString()}",
                flush=True
            )

    print("\nALL_8_DASHBOARD_SCHEMAS_PASSED", flush=True)
finally:
    spark.stop()
