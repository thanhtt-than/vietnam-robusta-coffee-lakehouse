from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Weather-Selection")
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
    df = spark.table("lakehouse.silver.weather_daily")

    priority = (
        F.when(
            (F.col("province") == "GIA_LAI") &
            (F.col("source") == "open_meteo_archive_gia_lai_corrected_v2"),
            1
        )
        .when(
            (F.col("province") == "LAM_DONG") &
            (F.col("source") == "open_meteo_archive_lam_dong_corrected_v2"),
            1
        )
        .when(
            (F.col("province") == "DAK_LAK") &
            (F.col("source") == "open_meteo_archive_dak_lak"),
            1
        )
        .when(
            (F.col("province") == "DAK_NONG") &
            (F.col("source") == "open_meteo_archive_dak_nong"),
            1
        )
        .when(
            (F.col("province").isin("DAK_LAK", "DAK_NONG")) &
            (F.col("source") == "open_meteo"),
            2
        )
        .otherwise(99)
    )

    eligible = (
        df.withColumn("priority", priority)
        .filter(F.col("priority") < 99)
    )

    window = Window.partitionBy(
        "observation_date", "province"
    ).orderBy("priority", "source_series_id")

    selected = (
        eligible.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
    )

    print("SELECTED_WEATHER_ROWS:", selected.count(), flush=True)

    print("SELECTED_BY_SOURCE:", flush=True)
    selected.groupBy("province", "source").count().orderBy(
        "province", "source"
    ).show(truncate=False)

    duplicates = (
        selected.groupBy("observation_date", "province")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("DUPLICATE_KEYS:", duplicates, flush=True)
    print("WEATHER_SELECTION_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
