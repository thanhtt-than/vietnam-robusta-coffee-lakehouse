import sys
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WRITE = "--write" in sys.argv[1:]

spark = (
    SparkSession.builder
    .appName("TLCN-Gold-Foundation-Dimensions")
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

try:
    # 1. Date range from real Silver data.
    date_inputs = [
        ("coffee_prices_daily", "quote_date"),
        ("fuel_price_observations", "quote_date"),
        ("weather_daily", "observation_date"),
        ("london_futures_daily", "trade_date"),
        ("vicofa_articles", "published_date"),
    ]

    date_df = None

    for table, col in date_inputs:
        part = spark.table(
            "lakehouse.silver." + table
        ).select(F.col(col).cast("date").alias("full_date"))

        date_df = (
            part if date_df is None
            else date_df.unionByName(part)
        )

    bounds = date_df.agg(
        F.min("full_date").alias("first_day"),
        F.max("full_date").alias("last_day")
    ).first()

    first_year = min(bounds["first_day"].year, 2001)
    last_year = bounds["last_day"].year

    start = date(first_year, 1, 1)
    end = date(last_year, 12, 31)
    day_count = (end - start).days + 1

    calendar = spark.range(day_count).select(
        F.date_add(
            F.lit(start).cast("date"),
            F.col("id").cast("int")
        ).alias("full_date")
    )

    dim_date = calendar.select(
        F.date_format("full_date", "yyyyMMdd")
         .cast("int").alias("date_key"),
        "full_date",
        F.month("full_date").alias("month"),
        F.quarter("full_date").alias("quarter"),
        F.year("full_date").alias("year")
    )

    dim_year = spark.range(
        first_year, last_year + 1
    ).select(
        F.col("id").cast("int").alias("year_key"),
        F.col("id").cast("int").alias("calendar_year")
    )

    # 2. Geography: country + four coffee provinces.
    geography_rows = [
        (1, "VN", "Viet Nam", "COUNTRY", "VIET_NAM"),
        (2, "DAK_LAK", "Dak Lak", "PROVINCE", "TAY_NGUYEN"),
        (3, "DAK_NONG", "Dak Nong", "PROVINCE", "TAY_NGUYEN"),
        (4, "GIA_LAI", "Gia Lai", "PROVINCE", "TAY_NGUYEN"),
        (5, "LAM_DONG", "Lam Dong", "PROVINCE", "TAY_NGUYEN"),
    ]

    dim_geography = spark.createDataFrame(
        geography_rows,
        """
        geography_key INT,
        geography_code STRING,
        geography_name STRING,
        geography_level STRING,
        region_name STRING
        """
    )

    # 3. Sources observed in Silver.
    source_inputs = [
        ("coffee_prices_daily", "source_family"),
        ("fuel_price_observations", "source_family"),
        ("weather_daily", "source"),
        ("london_futures_daily", "selected_source"),
        ("coffee_area_annual", "selected_source"),
        ("coffee_production_annual", "selected_source"),
        ("coffee_trade_annual", "selected_source"),
    ]

    sources = None

    for table, col in source_inputs:
        part = spark.table(
            "lakehouse.silver." + table
        ).select(
            F.col(col).alias("source_code")
        )

        sources = (
            part if sources is None
            else sources.unionByName(part)
        )

    sources = sources.unionByName(
        spark.createDataFrame(
            [("vicofa_website",)],
            ["source_code"]
        )
    )

    sources = (
        sources
        .filter(
            F.col("source_code").isNotNull()
            & (F.trim("source_code") != "")
        )
        .distinct()
    )

    # Deterministic BIGINT key from source code.
    dim_source = sources.select(
        F.expr(
            "CAST(CONV(SUBSTR(SHA2(source_code, 256),"
            " 1, 15), 16, 10) AS BIGINT)"
        ).alias("source_key"),
        F.col("source_code"),
        F.col("source_code").alias("source_name")
    )

    dimensions = {
        "dim_date": (
            dim_date,
            """
            date_key INT,
            full_date DATE,
            month INT,
            quarter INT,
            year INT
            """,
            ["date_key"]
        ),
        "dim_year": (
            dim_year,
            """
            year_key INT,
            calendar_year INT
            """,
            ["year_key"]
        ),
        "dim_geography": (
            dim_geography,
            """
            geography_key INT,
            geography_code STRING,
            geography_name STRING,
            geography_level STRING,
            region_name STRING
            """,
            ["geography_key"]
        ),
        "dim_source": (
            dim_source,
            """
            source_key BIGINT,
            source_code STRING,
            source_name STRING
            """,
            ["source_key"]
        )
    }

    # 4. Validate all dimensions before any write.
    for name, (df, ddl, keys) in dimensions.items():
        total = df.count()
        unique = df.select(*keys).distinct().count()

        null_condition = None
        for key in keys:
            check = F.col(key).isNull()
            null_condition = (
                check if null_condition is None
                else null_condition | check
            )

        null_count = df.filter(null_condition).count()

        print("DIMENSION:", name, flush=True)
        print("ROWS:", total, flush=True)
        print("UNIQUE_KEYS:", unique, flush=True)
        print("NULL_KEYS:", null_count, flush=True)

        assert total > 0
        assert unique == total
        assert null_count == 0

        if name in ("dim_geography", "dim_source"):
            df.orderBy(keys[0]).show(40, truncate=False)

    # Guard source hash collisions.
    source_code_count = dim_source.select(
        "source_code"
    ).distinct().count()

    source_key_count = dim_source.select(
        "source_key"
    ).distinct().count()

    assert source_code_count == source_key_count, (
        "Source key collision detected"
    )

    # 5. Write only when explicitly requested.
    if WRITE:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        for name, (df, ddl, keys) in dimensions.items():
            target = "lakehouse.gold." + name
            view = "stage_" + name

            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {target} "
                f"({ddl}) USING iceberg"
            )

            df.createOrReplaceTempView(view)

            key = keys[0]

            spark.sql(f"""
                MERGE INTO {target} t
                USING {view} s
                ON t.{key} = s.{key}
                WHEN NOT MATCHED THEN INSERT *
            """)

            actual = spark.table(target).count()
            expected = df.count()

            print(
                "GOLD_TABLE:", target,
                "ROWS:", actual,
                flush=True
            )

            assert actual == expected, (
                f"{name}: expected {expected}, got {actual}"
            )

            print(
                name.upper() + "_ICEBERG_PASSED",
                flush=True
            )

        print(
            "GOLD_FOUNDATION_ICEBERG_PASSED",
            flush=True
        )

    else:
        print(
            "GOLD_FOUNDATION_DRY_RUN_PASSED",
            flush=True
        )
        print("NO_GOLD_TABLE_WRITTEN", flush=True)

finally:
    spark.stop()
