import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "lakehouse.gold.mart_coffee_export_annual"


def main():
    spark = (
        SparkSession.builder
        .appName("Build-Mart-Coffee-Export-Annual")
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
        fact = spark.table("lakehouse.gold.fact_export")
        years = spark.table("lakehouse.gold.dim_year")
        destinations = spark.table("lakehouse.gold.dim_destination")
        products = spark.table("lakehouse.gold.dim_hs_product")
        geography = spark.table("lakehouse.gold.dim_geography")

        for name, df, key in [
            ("dim_year", years, "year_key"),
            ("dim_destination", destinations, "destination_key"),
            ("dim_hs_product", products, "hs_product_key"),
            ("dim_geography", geography, "geography_key"),
        ]:
            if df.count() != df.select(key).distinct().count():
                raise RuntimeError("Duplicate Dimension key: " + name)

        base = (
            fact.alias("f")
            .join(
                years.alias("y"),
                F.col("f.year_key") == F.col("y.year_key"),
                "left"
            )
            .join(
                destinations.alias("d"),
                F.col("f.destination_key")
                == F.col("d.destination_key"),
                "left"
            )
            .join(
                products.alias("p"),
                F.col("f.hs_product_key")
                == F.col("p.hs_product_key"),
                "left"
            )
            .join(
                geography.alias("g"),
                F.col("f.geography_key")
                == F.col("g.geography_key"),
                "left"
            )
            .select(
                F.col("f.export_fact_key"),
                F.col("f.source_key"),
                F.col("y.year_key"),
                F.col("y.calendar_year").alias("export_year"),
                F.col("g.geography_code").alias("geography_code"),
                F.col("f.reporter_code"),
                F.col("d.destination_key"),
                F.col("d.partner_code"),
                F.col("d.partner_name"),
                F.col("p.hs_product_key"),
                F.col("p.product_code"),
                F.col("p.product_name"),
                F.col("f.is_world_aggregate"),
                F.col("f.value_usd_thousand").alias(
                    "export_value_usd_thousand"
                ),
                F.col("f.quality_status"),
                F.col("f.raw_observation_count"),
                F.col("f.distinct_values_count")
            )
        )

        # One world total for each year/reporter/product.
        group_cols = [
            "export_year", "reporter_code",
            "geography_code", "hs_product_key"
        ]

        world = (
            base.filter(F.col("is_world_aggregate") == True)
            .groupBy(*group_cols)
            .agg(
                F.count("*").alias("world_row_count"),
                F.max("export_value_usd_thousand").alias(
                    "world_export_value_usd_thousand"
                )
            )
        )

        if world.filter(F.col("world_row_count") > 1).count():
            raise RuntimeError(
                "Multiple world totals for the same year/reporter/product"
            )

        base = base.join(
            world.select(
                *group_cols, "world_export_value_usd_thousand"
            ),
            group_cols,
            "left"
        )

        # YoY: calculate within each destination and product.
        history_window = (
            Window.partitionBy(
                "reporter_code",
                "geography_code",
                "destination_key",
                "hs_product_key"
            )
            .orderBy("export_year")
        )

        mart = (
            base
            .withColumn(
                "previous_export_year",
                F.lag("export_year").over(history_window)
            )
            .withColumn(
                "previous_export_value",
                F.lag("export_value_usd_thousand").over(
                    history_window
                )
            )
            .withColumn(
                "export_growth_yoy_pct",
                F.when(
                    (F.col("previous_export_year")
                     == F.col("export_year") - 1)
                    & (F.col("previous_export_value") > 0),
                    F.round(
                        (
                            F.col("export_value_usd_thousand")
                            / F.col("previous_export_value") - 1
                        ) * 100,
                        3
                    )
                )
            )
            .withColumn(
                "destination_share_pct",
                F.when(
                    (F.col("is_world_aggregate") == False)
                    & (F.col("world_export_value_usd_thousand") > 0),
                    F.round(
                        F.col("export_value_usd_thousand")
                        / F.col("world_export_value_usd_thousand")
                        * 100,
                        3
                    )
                )
            )
            .drop(
                "previous_export_year",
                "previous_export_value"
            )
        )

        # Rank destinations without mixing in world totals.
        rank_window = (
            Window.partitionBy(*group_cols)
            .orderBy(
                F.col("export_value_usd_thousand").desc_nulls_last()
            )
        )

        ranking = (
            mart.filter(F.col("is_world_aggregate") == False)
            .withColumn(
                "destination_rank",
                F.dense_rank().over(rank_window)
            )
            .select("export_fact_key", "destination_rank")
        )

        mart = mart.join(
            ranking, "export_fact_key", "left"
        )

        # Quality and grain checks.
        fact_rows = fact.count()
        mart_rows = mart.count()

        duplicate_keys = (
            mart.groupBy("export_fact_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        missing_dimensions = mart.filter(
            F.col("export_year").isNull()
            | F.col("partner_code").isNull()
            | F.col("product_code").isNull()
            | F.col("geography_code").isNull()
        ).count()

        invalid_values = mart.filter(
            F.col("export_value_usd_thousand").isNull()
            | (F.col("export_value_usd_thousand") < 0)
        ).count()

        invalid_share = mart.filter(
            (F.col("destination_share_pct") < 0)
            | (F.col("destination_share_pct") > 100)
        ).count()

        if (
            mart_rows == 0
            or mart_rows != fact_rows
            or duplicate_keys
            or missing_dimensions
            or invalid_values
            or invalid_share
        ):
            raise RuntimeError(
                f"Validation failed: fact={fact_rows}, "
                f"mart={mart_rows}, duplicates={duplicate_keys}, "
                f"missing_dims={missing_dimensions}, "
                f"invalid_values={invalid_values}, "
                f"invalid_share={invalid_share}"
            )

        print("EXPORT_FACT_ROWS:", fact_rows, flush=True)
        print("EXPORT_MART_ROWS:", mart_rows, flush=True)
        print("EXPORT_DUPLICATES:", duplicate_keys, flush=True)
        print("EXPORT_MART_VALIDATION_PASSED", flush=True)

        mart.select(
            "export_year",
            "partner_name",
            "product_code",
            "is_world_aggregate",
            "export_value_usd_thousand",
            "export_growth_yoy_pct",
            "destination_share_pct",
            "destination_rank"
        ).orderBy(
            F.col("export_year").desc(),
            F.col("export_value_usd_thousand").desc()
        ).show(15, truncate=False)

        if "--write" in sys.argv[1:]:
            if spark.catalog.tableExists(TARGET):
                raise RuntimeError("Target already exists: " + TARGET)

            mart.writeTo(TARGET).using("iceberg").create()

            actual = spark.table(TARGET).count()
            if actual != mart_rows:
                raise RuntimeError("Iceberg row count mismatch")

            print("EXPORT_ICEBERG_ROWS:", actual, flush=True)
            print("EXPORT_ICEBERG_WRITE_PASSED", flush=True)
        else:
            print("EXPORT_MART_DRY_RUN_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
