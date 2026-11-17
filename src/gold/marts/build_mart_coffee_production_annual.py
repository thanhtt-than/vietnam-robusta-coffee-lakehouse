import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TARGET = "lakehouse.gold.mart_coffee_production_annual"


def main():
    spark = (
        SparkSession.builder
        .appName("Build-Mart-Coffee-Production-Annual")
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
        fact = spark.table("lakehouse.gold.fact_production")
        years = spark.table("lakehouse.gold.dim_year")
        geography = spark.table("lakehouse.gold.dim_geography")

        # Prevent one-to-many joins from multiplying Fact rows.
        for name, df, key in [
            ("dim_year", years, "year_key"),
            ("dim_geography", geography, "geography_key"),
        ]:
            if df.count() != df.select(key).distinct().count():
                raise RuntimeError("Duplicate Dimension key: " + name)

        # Join production Fact with Year and Geography Dimensions.
        base = (
            fact.alias("f")
            .join(
                years.alias("y"),
                F.col("f.year_key") == F.col("y.year_key"),
                "left"
            )
            .join(
                geography.alias("g"),
                F.col("f.geography_key")
                == F.col("g.geography_key"),
                "left"
            )
            .select(
                F.col("f.production_fact_key"),
                F.col("f.year_key").alias("production_year"),
                F.col("g.geography_code"),
                F.col("g.geography_name"),
                F.col("g.geography_level"),
                F.col("f.coffee_area_thousand_ha"),
                F.col("f.coffee_production_thousand_tons"),
                F.col("f.area_growth_index_prev_year_100"),
                F.col("f.production_growth_index_prev_year_100"),
                F.col("f.area_publication_status"),
                F.col("f.production_publication_status"),
                F.col("f.area_quality_status"),
                F.col("f.production_quality_status"),
                F.col("f.area_raw_observation_count"),
                F.col("f.production_raw_observation_count"),
                F.col("y.year_key").alias("matched_year_key")
            )
        )

        # Derived analytical KPIs.
        mart = (
            base
            .withColumn(
                "yield_tons_per_ha",
                F.when(
                    F.col("coffee_area_thousand_ha") > 0,
                    F.round(
                        F.col("coffee_production_thousand_tons")
                        / F.col("coffee_area_thousand_ha"),
                        3
                    )
                )
            )
            .withColumn(
                "area_growth_pct",
                F.when(
                    F.col("area_growth_index_prev_year_100").isNotNull(),
                    F.round(
                        F.col("area_growth_index_prev_year_100") - 100,
                        3
                    )
                )
            )
            .withColumn(
                "production_growth_pct",
                F.when(
                    F.col("production_growth_index_prev_year_100").isNotNull(),
                    F.round(
                        F.col("production_growth_index_prev_year_100") - 100,
                        3
                    )
                )
            )
            .drop(
                "area_growth_index_prev_year_100",
                "production_growth_index_prev_year_100",
                "matched_year_key"
            )
        )

        # Validate source integrity and Data Mart grain.
        fact_rows = fact.count()
        mart_rows = mart.count()

        duplicate_keys = (
            mart.groupBy("production_year", "geography_code")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        missing_dimensions = base.filter(
            F.col("matched_year_key").isNull()
            | F.col("geography_code").isNull()
        ).count()

        invalid_values = mart.filter(
            (F.col("coffee_area_thousand_ha") < 0)
            | (F.col("coffee_production_thousand_tons") < 0)
        ).count()

        if (
            mart_rows == 0
            or mart_rows != fact_rows
            or duplicate_keys != 0
            or missing_dimensions != 0
            or invalid_values != 0
        ):
            raise RuntimeError(
                f"Validation failed: fact={fact_rows}, "
                f"mart={mart_rows}, duplicates={duplicate_keys}, "
                f"missing_dimensions={missing_dimensions}, "
                f"invalid={invalid_values}"
            )

        print("PRODUCTION_FACT_ROWS:", fact_rows, flush=True)
        print("PRODUCTION_MART_ROWS:", mart_rows, flush=True)
        print("PRODUCTION_DUPLICATES:", duplicate_keys, flush=True)
        print("PRODUCTION_MART_VALIDATION_PASSED", flush=True)

        mart.orderBy("production_year").show(25, truncate=False)

        if "--write" in sys.argv[1:]:
            if spark.catalog.tableExists(TARGET):
                raise RuntimeError("Target already exists: " + TARGET)

            mart.writeTo(TARGET).using("iceberg").create()

            actual = spark.table(TARGET).count()
            if actual != mart_rows:
                raise RuntimeError("Iceberg row count mismatch")

            print("PRODUCTION_ICEBERG_ROWS:", actual, flush=True)
            print("PRODUCTION_ICEBERG_WRITE_PASSED", flush=True)
        else:
            print("PRODUCTION_MART_DRY_RUN_PASSED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
