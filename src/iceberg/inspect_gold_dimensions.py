from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Dimension-Readiness")
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

checks = {
    "coffee_prices_daily": [
        "source_family", "province", "quality_status"
    ],
    "fuel_price_observations": [
        "source_family", "series_code", "region", "unit"
    ],
    "weather_daily": [
        "source", "province", "source_series_id",
        "source_timezone"
    ],
    "london_futures_daily": [],
    "coffee_trade_annual": [
        "reporter_code", "product_code", "product_name"
    ],
    "vicofa_articles": []
}

try:
    for name, columns in checks.items():
        print("\n===== " + name.upper() + " =====", flush=True)

        df = spark.table("lakehouse.silver." + name)
        df.printSchema()

        for col in columns:
            if col not in df.columns:
                print("MISSING_COLUMN:", col, flush=True)
                continue

            print("DISTINCT_VALUES:", col, flush=True)

            (
                df.groupBy(col)
                .count()
                .orderBy(F.desc("count"), col)
                .show(25, truncate=False)
            )

    print(
        "\nGOLD_DIMENSION_INSPECTION_PASSED",
        flush=True
    )

finally:
    spark.stop()
