from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLCN-Verify-All-Silver")
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

TABLES = {
    "coffee_prices_daily": {
        "expected": 9409,
        "key": ["source_family", "province", "quote_date"],
        "bronze": ["coffee_prices_raw", "domestic_prices_raw"]
    },
    "fuel_price_observations": {
        "expected": 1704,
        "key": [
            "source_family", "series_code",
            "region", "quote_date", "unit"
        ],
        "bronze": ["fuel_prices_raw"]
    },
    "weather_daily": {
        "expected": 97620,
        "key": ["source_series_id", "observation_date"],
        "bronze": ["weather_raw"]
    },
    "london_futures_daily": {
        "expected": 4755,
        "key": ["trade_date"],
        "bronze": ["london_futures_raw"]
    },
    "coffee_area_annual": {
        "expected": 19,
        "key": ["year"],
        "bronze": ["coffee_area_raw"]
    },
    "coffee_production_annual": {
        "expected": 19,
        "key": ["year"],
        "bronze": ["coffee_production_raw"]
    },
    "coffee_trade_annual": {
        "expected": 3888,
        "key": [
            "reporter_code", "partner_code",
            "product_code", "year"
        ],
        "bronze": ["coffee_trade_raw"]
    },
    "vicofa_articles": {
        "expected": 1218,
        "key": ["article_id"],
        "bronze": ["vicofa_articles_raw"]
    }
}

results = []
errors = []

try:
    for name, config in TABLES.items():
        table_name = "lakehouse.silver." + name

        print("\n" + "=" * 65, flush=True)
        print("VERIFY_TABLE:", table_name, flush=True)

        try:
            df = spark.table(table_name)

            total = df.count()
            expected = config["expected"]
            keys = config["key"]

            print("ROWS:", total, flush=True)
            print("EXPECTED_ROWS:", expected, flush=True)

            if total != expected:
                errors.append(
                    name + ": unexpected row count"
                )

            # Kiem tra cot khoa
            missing_key_columns = [
                c for c in keys if c not in df.columns
            ]

            if missing_key_columns:
                errors.append(
                    name + ": missing key columns "
                    + str(missing_key_columns)
                )
                unique_count = None
                null_key_count = None

            else:
                unique_count = (
                    df.select(*keys)
                    .distinct()
                    .count()
                )

                null_condition = None

                for key in keys:
                    condition = F.col(key).isNull()

                    if null_condition is None:
                        null_condition = condition
                    else:
                        null_condition = (
                            null_condition | condition
                        )

                null_key_count = (
                    df.filter(null_condition).count()
                )

                print(
                    "UNIQUE_KEYS:", unique_count,
                    flush=True
                )
                print(
                    "NULL_KEY_ROWS:", null_key_count,
                    flush=True
                )

                if unique_count != total:
                    errors.append(
                        name + ": duplicate business keys"
                    )

                if null_key_count != 0:
                    errors.append(
                        name + ": null business keys"
                    )

            # Kiem tra trang thai chat luong
            if "quality_status" in df.columns:
                print("QUALITY_STATUS:", flush=True)

                df.groupBy(
                    "quality_status"
                ).count().orderBy(
                    "quality_status"
                ).show(truncate=False)

                null_quality = df.filter(
                    F.col("quality_status").isNull()
                ).count()

                if null_quality > 0:
                    errors.append(
                        name + ": null quality status"
                    )

            # Kiem tra cot lineage
            lineage_columns = [
                c for c in df.columns
                if c == "selected_bronze_record_id"
                or c.endswith("_bronze_record_id")
            ]

            print(
                "LINEAGE_COLUMNS:",
                lineage_columns,
                flush=True
            )

            if not lineage_columns:
                print(
                    "LINEAGE_CHECK_SKIPPED:"
                    " no recognized Bronze ID column",
                    flush=True
                )

            else:
                # Lay ID goc tu Bronze Iceberg
                bronze_ids = None

                for bronze_name in config["bronze"]:
                    bronze_df = (
                        spark.table(
                            "lakehouse.bronze." + bronze_name
                        )
                        .select("record_id")
                        .distinct()
                    )

                    if bronze_ids is None:
                        bronze_ids = bronze_df
                    else:
                        bronze_ids = (
                            bronze_ids.unionByName(bronze_df)
                        )

                bronze_ids = (
                    bronze_ids.distinct()
                    .withColumnRenamed(
                        "record_id", "bronze_id"
                    )
                )

                for lineage_col in lineage_columns:
                    null_refs = df.filter(
                        F.col(lineage_col).isNull()
                    ).count()

                    source_refs = (
                        df.select(
                            F.col(lineage_col)
                            .alias("bronze_id")
                        )
                        .filter(
                            F.col("bronze_id").isNotNull()
                        )
                        .distinct()
                    )

                    unmatched = (
                        source_refs.join(
                            bronze_ids,
                            on="bronze_id",
                            how="left_anti"
                        )
                        .count()
                    )

                    print(
                        "LINEAGE_FIELD:", lineage_col,
                        flush=True
                    )
                    print(
                        "LINEAGE_NULL_REFS:", null_refs,
                        flush=True
                    )
                    print(
                        "LINEAGE_UNMATCHED_REFS:",
                        unmatched,
                        flush=True
                    )

                    if unmatched > 0:
                        errors.append(
                            name + ": unmatched Bronze IDs in "
                            + lineage_col
                        )

                    if null_refs > 0:
                        print(
                            "LINEAGE_WARNING:"
                            " some Silver rows have no Bronze ID",
                            flush=True
                        )

            results.append({
                "table": name,
                "rows": total,
                "expected": expected,
                "unique_keys": unique_count,
                "null_keys": null_key_count
            })

            print(
                "TABLE_CHECK_FINISHED:", name,
                flush=True
            )

        except Exception as exc:
            errors.append(name + ": " + str(exc))
            print(
                "TABLE_CHECK_ERROR:",
                name, str(exc),
                flush=True
            )

    print("\n===== SILVER SUMMARY =====", flush=True)

    for item in results:
        print(
            item["table"],
            "ROWS=", item["rows"],
            "UNIQUE=", item["unique_keys"],
            "NULL_KEYS=", item["null_keys"],
            flush=True
        )

    print(
        "SILVER_TABLES_CHECKED:",
        len(results),
        flush=True
    )

    print(
        "SILVER_TOTAL_ROWS:",
        sum(x["rows"] for x in results),
        flush=True
    )

    print(
        "SILVER_VALIDATION_ERRORS:",
        len(errors),
        flush=True
    )

    if errors:
        print("VALIDATION_ERROR_DETAILS:", flush=True)

        for error in errors:
            print(" -", error, flush=True)

        raise RuntimeError(
            "SILVER_VALIDATION_FAILED"
        )

    print(
        "ALL_SILVER_TABLES_VERIFIED",
        flush=True
    )

finally:
    spark.stop()
