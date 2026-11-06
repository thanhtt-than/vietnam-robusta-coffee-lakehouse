import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WRITE = "--write" in sys.argv[1:]

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Market-Dimensions")
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


def make_key(column):
    return F.expr(
        "CAST(CONV(SUBSTR(SHA2("
        + column +
        ", 256), 1, 15), 16, 10) AS BIGINT)"
    )


def verify(df, key, natural_key, name):
    total = df.count()
    unique = df.select(key).distinct().count()
    unique_natural = df.select(natural_key).distinct().count()

    nulls = df.filter(
        F.col(key).isNull()
        | F.col(natural_key).isNull()
    ).count()

    print("DIMENSION:", name, flush=True)
    print("ROWS:", total, flush=True)
    print("UNIQUE_KEYS:", unique, flush=True)
    print("UNIQUE_NATURAL_KEYS:", unique_natural, flush=True)
    print("NULL_KEYS:", nulls, flush=True)

    assert total > 0
    assert unique == total
    assert unique_natural == total
    assert nulls == 0

    return total


try:
    coffee = spark.table(
        "lakehouse.silver.coffee_prices_daily"
    )

    weather = spark.table(
        "lakehouse.silver.weather_daily"
    )

    fuel = spark.table(
        "lakehouse.silver.fuel_price_observations"
    )

    # 1. DIM_PRICE_SERIES
    price_series = (
        coffee.select(
            "source_family",
            F.col("province").alias("province_code")
        )
        .distinct()
        .withColumn(
            "series_code",
            F.concat_ws(
                "|", "source_family", "province_code"
            )
        )
        .withColumn(
            "price_series_key",
            make_key("series_code")
        )
        .withColumn(
            "quote_type",
            F.lit("DAILY_PRICE_VND_KG")
        )
        .withColumn(
            "market",
            F.lit("VN_DOMESTIC")
        )
        .select(
            "price_series_key",
            "series_code",
            "source_family",
            "province_code",
            "quote_type",
            "market"
        )
    )

    # 2. DIM_WEATHER_LOCATION
    weather_location = (
        weather.select(
            "source_series_id",
            "source",
            F.col("province").alias("province_code"),
            "latitude",
            "longitude",
            "elevation_m",
            "source_timezone"
        )
        .distinct()
        .withColumn(
            "weather_location_key",
            make_key("source_series_id")
        )
        .withColumn(
            "is_corrected",
            F.col("source").contains("corrected_v2")
        )
        .select(
            "weather_location_key",
            "source_series_id",
            "source",
            "province_code",
            "latitude",
            "longitude",
            "elevation_m",
            "source_timezone",
            "is_corrected"
        )
    )

    # 3. DIM_FUEL_SERIES
    fuel_series = (
        fuel.select(
            "source_family",
            "series_code",
            "region",
            "unit"
        )
        .distinct()
        .withColumn(
            "fuel_series_id",
            F.concat_ws(
                "|",
                "source_family",
                "series_code",
                "region",
                "unit"
            )
        )
        .withColumn(
            "fuel_series_key",
            make_key("fuel_series_id")
        )
        .select(
            "fuel_series_key",
            "fuel_series_id",
            "source_family",
            "series_code",
            "region",
            "unit"
        )
    )

    dimensions = {
        "dim_price_series": (
            price_series,
            "price_series_key",
            "series_code",
            """
            price_series_key BIGINT,
            series_code STRING,
            source_family STRING,
            province_code STRING,
            quote_type STRING,
            market STRING
            """
        ),
        "dim_weather_location": (
            weather_location,
            "weather_location_key",
            "source_series_id",
            """
            weather_location_key BIGINT,
            source_series_id STRING,
            source STRING,
            province_code STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            elevation_m DOUBLE,
            source_timezone STRING,
            is_corrected BOOLEAN
            """
        ),
        "dim_fuel_series": (
            fuel_series,
            "fuel_series_key",
            "fuel_series_id",
            """
            fuel_series_key BIGINT,
            fuel_series_id STRING,
            source_family STRING,
            series_code STRING,
            region STRING,
            unit STRING
            """
        )
    }

    counts = {}

    for name, (df, key, natural, ddl) in dimensions.items():
        counts[name] = verify(
            df, key, natural, name
        )

    assert counts["dim_weather_location"] == 12

    print("FUEL_SERIES_BY_SOURCE:", flush=True)
    fuel_series.groupBy("source_family").count().show(
        truncate=False
    )

    print("GOLD_MARKET_DIMENSIONS_VALIDATED", flush=True)

    if WRITE:
        # Chặn ghi nếu chưa có Dimension nền tảng.
        for foundation in [
            "dim_date",
            "dim_year",
            "dim_geography",
            "dim_source"
        ]:
            target = "lakehouse.gold." + foundation

            if not spark.catalog.tableExists(target):
                raise RuntimeError(
                    "Missing foundation dimension: " + target
                )

        print("GOLD_FOUNDATION_TABLES_FOUND", flush=True)

        for name, (df, key, natural, ddl) in dimensions.items():
            target = "lakehouse.gold." + name
            view = "stage_" + name

            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {target} "
                f"({ddl}) USING iceberg"
            )

            df.createOrReplaceTempView(view)

            # Không âm thầm ghi vào schema cũ khác cột.
            if spark.table(target).columns != df.columns:
                raise RuntimeError(
                    "Schema mismatch: " + target
                )

            spark.sql(f"""
                MERGE INTO {target} t
                USING {view} s
                ON t.{key} = s.{key}
                WHEN NOT MATCHED THEN INSERT *
            """)

            actual = spark.table(target).count()

            assert actual == counts[name]

            print(
                name.upper() + "_ICEBERG_TOTAL:",
                actual,
                flush=True
            )

            print(
                name.upper() + "_ICEBERG_PASSED",
                flush=True
            )

        print(
            "GOLD_MARKET_DIMENSIONS_ICEBERG_PASSED",
            flush=True
        )

    else:
        print(
            "GOLD_MARKET_DIMENSIONS_DRY_RUN_PASSED",
            flush=True
        )
        print("NO_GOLD_TABLE_WRITTEN", flush=True)

finally:
    spark.stop()
