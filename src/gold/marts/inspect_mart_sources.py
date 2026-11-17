
"""Inspect Silver sources for Gold Data Mart design.

READ-ONLY: No Bronze, Silver or Gold tables are modified.

Run:
    .\run_spark.ps1 -Script "src/gold/marts/inspect_mart_sources.py"
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TABLES = {
    "COFFEE": "lakehouse.silver.coffee_prices_daily",
    "WEATHER": "lakehouse.silver.weather_daily",
    "FUEL": "lakehouse.silver.fuel_price_observations",
    "FUTURES": "lakehouse.silver.london_futures_daily",
    "NEWS": "lakehouse.silver.vicofa_articles",
}

DATE_COLUMNS = [
    "quote_date",
    "observation_date",
    "trade_date",
    "published_date",
]

GROUP_COLUMNS = {
    "COFFEE": [
        "source_family",
        "province",
        "quality_status",
    ],
    "WEATHER": [
        "source",
        "province",
        "source_series_id",
        "quality_status",
    ],
    "FUEL": [
        "source_family",
        "series_code",
        "region",
        "unit",
        "quality_status",
    ],
    "FUTURES": [
        "quality_status",
    ],
    "NEWS": [
        "primary_category",
        "quality_status",
    ],
}


def create_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Inspect-Gold-Mart-Sources")
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.lakehouse.type",
            "rest",
        )
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def inspect_source(spark, name, table):
    print("\n" + "=" * 70)
    print("SOURCE:", name)
    print("TABLE:", table)
    print("=" * 70, flush=True)

    df = spark.table(table)

    print("\nSCHEMA:", flush=True)
    df.printSchema()

    print("TOTAL_ROWS:", df.count(), flush=True)

    # 1. Date coverage
    date_col = next(
        (c for c in DATE_COLUMNS if c in df.columns),
        None,
    )

    if date_col:
        print("\nDATE_COVERAGE:", flush=True)

        df.agg(
            F.min(date_col).alias("first_date"),
            F.max(date_col).alias("last_date"),
            F.countDistinct(date_col).alias("distinct_dates"),
        ).show(truncate=False)

    # 2. Group summaries
    for column in GROUP_COLUMNS[name]:
        if column not in df.columns:
            continue

        print("\nGROUP_BY:", column, flush=True)

        (
            df.groupBy(column)
            .count()
            .orderBy(F.desc("count"))
            .show(30, truncate=False)
        )

    # 3. Coffee source / province coverage
    if name == "COFFEE":
        print("\nCOFFEE_SOURCE_PROVINCE:", flush=True)

        (
            df.groupBy("source_family", "province")
            .agg(
                F.count("*").alias("records"),
                F.min("quote_date").alias("first_date"),
                F.max("quote_date").alias("last_date"),
                F.countDistinct("quote_date").alias("distinct_days"),
            )
            .orderBy("source_family", "province")
            .show(100, truncate=False)
        )

    # 4. Weather stations
    if name == "WEATHER":
        print("\nWEATHER_STATIONS:", flush=True)

        columns = [
            "source",
            "province",
            "source_series_id",
            "latitude",
            "longitude",
            "elevation_m",
            "source_timezone",
        ]

        available = [c for c in columns if c in df.columns]

        (
            df.select(*available)
            .distinct()
            .orderBy("province", "source")
            .show(100, truncate=False)
        )

    # 5. Fuel series
    if name == "FUEL":
        print("\nFUEL_SERIES:", flush=True)

        columns = [
            "source_family",
            "series_code",
            "region",
            "unit",
        ]

        available = [c for c in columns if c in df.columns]

        (
            df.select(*available)
            .distinct()
            .orderBy("series_code", "source_family")
            .show(100, truncate=False)
        )

    print(
        "\nSOURCE_INSPECTION_PASSED:", name,
        flush=True,
    )


def main():
    spark = create_spark()

    try:
        for name, table in TABLES.items():
            inspect_source(spark, name, table)

        print(
            "\nMART_SOURCE_INSPECTION_PASSED",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
