from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Gold-KPI-Type")
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

try:
    import sys
    sys.path.insert(0, "/workspace")

    from src.gold.marts.build_mart_coffee_market_daily import (
        prepare_coffee,
        add_coffee_kpis,
    )

    coffee = prepare_coffee(spark)
    result = add_coffee_kpis(coffee)

    actual_type = result.schema["coffee_change_pct"].dataType.simpleString()

    print("COFFEE_CHANGE_PCT_TYPE:", actual_type, flush=True)

    if actual_type != "decimal(12,3)":
        raise RuntimeError(
            f"Expected decimal(12,3), found {actual_type}"
        )

    print("COFFEE_KPI_TYPE_VERIFIED", flush=True)
    print("GOLD_WRITE_SKIPPED", flush=True)

finally:
    spark.stop()

