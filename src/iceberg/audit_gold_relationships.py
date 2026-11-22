"""
READ-ONLY audit of Gold tables and their upstream/downstream links.

Run from repository root:
    .\run_spark.ps1 -Script "src/iceberg/audit_gold_relationships.py"

This script does not write, alter, delete, or register any table.
It checks:
  1. Expected Gold catalog entries and row counts.
  2. Gold primary/natural key uniqueness.
  3. Gold foreign keys against Dimension tables.
  4. Silver -> Bronze record-id lineage.
  5. Gold -> Bronze record-id lineage.
  6. Selected Gold -> Silver record-id links.
  7. Production v1/v2 table key consistency.
"""

from functools import reduce

from pyspark.sql import SparkSession, functions as F


EXPECTED_GOLD = [
    "dim_article",
    "dim_coffee_exporter",
    "dim_coffee_product",
    "dim_currency_pair",
    "dim_date",
    "dim_destination",
    "dim_fuel_series",
    "dim_geography",
    "dim_hs_product",
    "dim_news_topic",
    "dim_port",
    "dim_price_series",
    "dim_source",
    "dim_weather_location",
    "dim_year",
    "fact_coffee_trade_monthly",
    "fact_domestic_price",
    "fact_exchange_rate",
    "fact_export",
    "fact_fuel_price",
    "fact_futures_price",
    "fact_global_coffee_exports",
    "fact_news_topic",
    "fact_production",
    "fact_production_v1_backup",
    "fact_production_v2_staging",
    "fact_psd",
    "fact_robusta_stock_port",
    "fact_robusta_stock_totals",
    "fact_weather_daily",
    "mart_coffee_production_annual",
]


PK = {
    "dim_article": "article_key",
    "dim_coffee_exporter": "exporter_key",
    "dim_coffee_product": "coffee_product_key",
    "dim_currency_pair": "currency_pair_key",
    "dim_date": "date_key",
    "dim_destination": "destination_key",
    "dim_fuel_series": "fuel_series_key",
    "dim_geography": "geography_key",
    "dim_hs_product": "hs_product_key",
    "dim_news_topic": "topic_key",
    "dim_port": "port_key",
    "dim_price_series": "price_series_key",
    "dim_source": "source_key",
    "dim_weather_location": "weather_location_key",
    "dim_year": "year_key",
    "fact_coffee_trade_monthly": "trade_fact_key",
    "fact_domestic_price": "price_fact_key",
    "fact_exchange_rate": "observation_id",
    "fact_export": "export_fact_key",
    "fact_fuel_price": "fuel_fact_key",
    "fact_futures_price": "futures_fact_key",
    "fact_global_coffee_exports": "export_fact_id",
    "fact_news_topic": "news_topic_fact_key",
    "fact_production": "production_fact_key",
    "fact_production_v1_backup": "production_fact_key",
    "fact_production_v2_staging": "production_fact_key",
    "fact_psd": "psd_fact_key",
    "fact_robusta_stock_port": "stock_port_fact_key",
    "fact_robusta_stock_totals": "stock_totals_fact_key",
    "fact_weather_daily": "weather_fact_key",
}

NATURAL_KEYS = {
    "dim_article": "article_id",
    "dim_coffee_exporter": "exporter_code",
    "dim_coffee_product": "product_code",
    "dim_currency_pair": "currency_pair",
    "dim_date": "full_date",
    "dim_destination": "partner_code",
    "dim_fuel_series": "fuel_series_id",
    "dim_geography": "geography_code",
    "dim_hs_product": "product_code",
    "dim_news_topic": "topic_code",
    "dim_port": "port_id",
    "dim_price_series": "series_code",
    "dim_source": "source_code",
    "dim_weather_location": "source_series_id",
    "dim_year": "calendar_year",
}

# A column is tested as a foreign key only when it exists in the Gold table.
# Nullable source keys in production v2 are reported but not treated as broken
# references; every non-null value must still resolve to the Dimension.
FK_TARGETS = {
    "date_key": ("dim_date", "date_key"),
    "trade_date_key": ("dim_date", "date_key"),
    "year_key": ("dim_year", "year_key"),
    "market_year_key": ("dim_year", "year_key"),
    "calendar_year_key": ("dim_year", "year_key"),
    "geography_key": ("dim_geography", "geography_key"),
    "source_key": ("dim_source", "source_key"),
    "area_source_key": ("dim_source", "source_key"),
    "production_source_key": ("dim_source", "source_key"),
    "yield_source_key": ("dim_source", "source_key"),
    "price_series_key": ("dim_price_series", "price_series_key"),
    "weather_location_key": (
        "dim_weather_location", "weather_location_key"
    ),
    "fuel_series_key": ("dim_fuel_series", "fuel_series_key"),
    "destination_key": ("dim_destination", "destination_key"),
    "hs_product_key": ("dim_hs_product", "hs_product_key"),
    "article_key": ("dim_article", "article_key"),
    "topic_key": ("dim_news_topic", "topic_key"),
    "port_key": ("dim_port", "port_key"),
    "coffee_product_key": (
        "dim_coffee_product", "coffee_product_key"
    ),
    "exporter_key": ("dim_coffee_exporter", "exporter_key"),
    "currency_pair_key": (
        "dim_currency_pair", "currency_pair_key"
    ),
}

# Direct record-level lineage from selected Gold facts to their Silver source.
GOLD_SILVER_LINKS = [
    (
        "fact_coffee_trade_monthly",
        "silver_observation_id",
        "coffee_trade_monthly",
        "observation_id",
    ),
    (
        "fact_exchange_rate",
        "observation_id",
        "exchange_rate_observations",
        "observation_id",
    ),
    (
        "fact_global_coffee_exports",
        "export_fact_id",
        "global_coffee_exports",
        "observation_id",
    ),
    (
        "fact_psd",
        "silver_record_id",
        "coffee_psd_observations",
        "silver_record_id",
    ),
]

# Bronze source names are not assumed to be identical to Silver names.
# Record IDs are also checked against the union of all Bronze record IDs below.
SILVER_TO_BRONZE_HINTS = {
    "coffee_area_annual": ["coffee_area_raw", "coffee_statistics_raw"],
    "coffee_prices_daily": ["coffee_prices_raw", "domestic_prices_raw"],
    "coffee_production_annual": [
        "coffee_production_raw", "coffee_statistics_raw"
    ],
    "coffee_psd_observations": ["coffee_psd_raw"],
    "coffee_statistics_observations": ["coffee_statistics_raw"],
    "coffee_trade_annual": ["coffee_trade_raw"],
    "coffee_trade_monthly": ["coffee_trade_monthly_raw"],
    "exchange_rate_observations": ["exchange_rate_raw"],
    "fuel_price_observations": ["fuel_prices_raw"],
    "london_futures_daily": ["london_futures_raw"],
    "robusta_stock_port_daily": ["robusta_stock_raw"],
    "robusta_stock_totals_daily": ["robusta_stock_raw"],
    "vicofa_articles": ["vicofa_articles_raw"],
    "weather_daily": ["weather_raw"],
    "global_coffee_exports": ["global_coffee_exports_raw"],
}

OPTIONAL_FKS = {
    ("fact_news_topic", "date_key"),
    ("fact_production", "area_source_key"),
    ("fact_production", "production_source_key"),
    ("fact_production_v2_staging", "area_source_key"),
    ("fact_production_v2_staging", "production_source_key"),
    ("fact_production_v2_staging", "yield_source_key"),
    ("fact_production_v1_backup", "area_source_key"),
    ("fact_production_v1_backup", "production_source_key"),
}


def make_spark():
    return (
        SparkSession.builder
        .appName("TLCN-Audit-Gold-Relationships-ReadOnly")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def table_names(spark, namespace):
    return {
        row["tableName"].lower()
        for row in spark.sql(
            f"SHOW TABLES IN lakehouse.{namespace}"
        ).collect()
    }


def table_df(spark, namespace, name):
    return spark.table(f"lakehouse.{namespace}.{name}")


def union_ids(spark, namespace, names, column_name):
    parts = []
    for name in sorted(names):
        df = table_df(spark, namespace, name)
        if column_name in df.columns:
            parts.append(
                df.select(
                    F.col(column_name).cast("string").alias("_id")
                ).filter(F.col("_id").isNotNull())
            )

    if not parts:
        return None

    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)

    return result.distinct()


def audit_reference(source_df, source_col, target_ids, label):
    refs = (
        source_df.select(
            F.col(source_col).cast("string").alias("_id")
        )
        .filter(F.col("_id").isNotNull())
        .distinct()
    )
    referenced = refs.count()

    if target_ids is None:
        print(
            "LINEAGE_NOT_CHECKED:",
            label,
            "reason=target_id_column_not_found",
            flush=True,
        )
        return None

    missing = refs.join(
        target_ids,
        on="_id",
        how="left_anti",
    ).count()

    print(
        "LINEAGE:",
        label,
        "DISTINCT_REFERENCES:",
        referenced,
        "UNMATCHED:",
        missing,
        flush=True,
    )
    return missing


def main():
    spark = make_spark()
    spark.sparkContext.setLogLevel("ERROR")

    failures = []
    warnings = []

    try:
        print("===== CATALOG INVENTORY =====", flush=True)

        actual_bronze = table_names(spark, "bronze")
        actual_silver = table_names(spark, "silver")
        actual_gold = table_names(spark, "gold")

        print("BRONZE_TABLE_COUNT:", len(actual_bronze), flush=True)
        print("SILVER_TABLE_COUNT:", len(actual_silver), flush=True)
        print("GOLD_TABLE_COUNT:", len(actual_gold), flush=True)

        missing_gold = sorted(set(EXPECTED_GOLD) - actual_gold)
        unexpected_gold = sorted(actual_gold - set(EXPECTED_GOLD))

        print("EXPECTED_GOLD_TABLES:", len(EXPECTED_GOLD), flush=True)
        print("MISSING_GOLD_TABLES:", missing_gold, flush=True)
        print("UNEXPECTED_GOLD_TABLES:", unexpected_gold, flush=True)

        if missing_gold:
            failures.append("Missing Gold catalog tables: " + str(missing_gold))

        # 1. Table counts, primary keys, natural keys and Iceberg snapshots.
        print("\n===== GOLD TABLE / PRIMARY KEY AUDIT =====", flush=True)

        gold_dfs = {}

        for name in sorted(actual_gold):
            df = table_df(spark, "gold", name)
            gold_dfs[name] = df
            rows = df.count()

            print(
                "GOLD_TABLE:",
                name,
                "ROWS:",
                rows,
                "COLUMNS:",
                len(df.columns),
                flush=True,
            )

            if rows == 0:
                failures.append(f"Gold table is empty: {name}")

            pk = PK.get(name)
            if pk:
                if pk not in df.columns:
                    failures.append(
                        f"{name}: configured primary key column missing: {pk}"
                    )
                else:
                    null_pk = df.filter(F.col(pk).isNull()).count()
                    unique_pk = df.select(pk).distinct().count()
                    duplicates = rows - unique_pk

                    print(
                        "GOLD_PK:",
                        name,
                        pk,
                        "NULL:",
                        null_pk,
                        "DUPLICATE_ROWS:",
                        duplicates,
                        flush=True,
                    )

                    if null_pk or duplicates:
                        failures.append(
                            f"{name}: PK invalid null={null_pk}, duplicates={duplicates}"
                        )

            natural = NATURAL_KEYS.get(name)
            if natural and natural in df.columns:
                null_natural = df.filter(
                    F.col(natural).isNull()
                ).count()
                unique_natural = df.select(natural).distinct().count()
                duplicates = rows - unique_natural

                print(
                    "GOLD_NATURAL_KEY:",
                    name,
                    natural,
                    "NULL:",
                    null_natural,
                    "DUPLICATE_ROWS:",
                    duplicates,
                    flush=True,
                )

                if null_natural or duplicates:
                    failures.append(
                        f"{name}: natural key invalid null={null_natural}, duplicates={duplicates}"
                    )

            # Iceberg metadata-table check. Unsupported metadata queries are
            # warnings, not proof that the table is corrupt.
            try:
                snapshots = spark.table(
                    f"lakehouse.gold.{name}.snapshots"
                )
                snapshot_count = snapshots.count()
                latest = (
                    snapshots.orderBy(
                        F.col("committed_at").desc()
                    ).select("snapshot_id").first()
                )
                latest_id = latest["snapshot_id"] if latest else None

                print(
                    "ICEBERG_SNAPSHOT:",
                    name,
                    "COUNT:",
                    snapshot_count,
                    "LATEST:",
                    latest_id,
                    flush=True,
                )

                if snapshot_count == 0:
                    failures.append(
                        f"{name}: Iceberg table has no active snapshot"
                    )
            except Exception as exc:
                warnings.append(
                    f"{name}: snapshot metadata query unavailable: {str(exc)[:180]}"
                )

        # 2. Foreign keys for every Gold table, including the newly registered
        # tables and any mart/staging/backup tables.
        print("\n===== GOLD FOREIGN KEY AUDIT =====", flush=True)

        for name, df in sorted(gold_dfs.items()):
            for fk, (dim_name, dim_pk) in FK_TARGETS.items():
                if fk not in df.columns:
                    continue

                # Do not treat a dimension's own primary key as its FK.
                if name == dim_name:
                    continue

                if dim_name not in gold_dfs:
                    failures.append(
                        f"{name}.{fk}: referenced dimension missing: {dim_name}"
                    )
                    continue

                dim = gold_dfs[dim_name]
                if dim_pk not in dim.columns:
                    failures.append(
                        f"{dim_name}: referenced key column missing: {dim_pk}"
                    )
                    continue

                nulls = df.filter(F.col(fk).isNull()).count()

                unmatched = (
                    df.filter(F.col(fk).isNotNull())
                    .select(F.col(fk).alias("_fk"))
                    .distinct()
                    .join(
                        dim.select(F.col(dim_pk).alias("_pk")).distinct(),
                        F.col("_fk") == F.col("_pk"),
                        "left_anti",
                    )
                    .count()
                )

                optional = (name, fk) in OPTIONAL_FKS

                print(
                    "GOLD_FK:",
                    name + "." + fk,
                    "->",
                    dim_name + "." + dim_pk,
                    "NULL:",
                    nulls,
                    "UNMATCHED:",
                    unmatched,
                    "OPTIONAL:",
                    optional,
                    flush=True,
                )

                if unmatched:
                    failures.append(
                        f"{name}.{fk}: {unmatched} non-null FK values do not exist in {dim_name}.{dim_pk}"
                    )

                if nulls and not optional:
                    warnings.append(
                        f"{name}.{fk}: {nulls} NULL FK values; review business rule"
                    )

        # 3. Silver -> Bronze lineage. Each Silver field that explicitly
        # stores a Bronze record ID must resolve to an existing Bronze ID.
        print("\n===== SILVER TO BRONZE LINEAGE AUDIT =====", flush=True)

        bronze_ids = union_ids(
            spark, "bronze", actual_bronze, "record_id"
        )

        if bronze_ids is None:
            failures.append("No Bronze table exposes record_id")
        else:
            bronze_id_count = bronze_ids.count()
            print("BRONZE_UNIQUE_RECORD_IDS:", bronze_id_count, flush=True)

        for name in sorted(actual_silver):
            df = table_df(spark, "silver", name)
            lineage_cols = [
                c for c in df.columns
                if "bronze" in c.lower() and "record_id" in c.lower()
            ]

            if not lineage_cols:
                warnings.append(
                    f"Silver table {name} has no explicit Bronze record-id column"
                )
                print(
                    "SILVER_LINEAGE_COLUMNS:",
                    name,
                    "NONE",
                    flush=True,
                )
                continue

            for column in lineage_cols:
                missing = audit_reference(
                    df,
                    column,
                    bronze_ids,
                    f"silver.{name}.{column} -> all bronze record_id",
                )
                if missing:
                    failures.append(
                        f"silver.{name}.{column}: {missing} references not found in Bronze"
                    )

        # Also validate Bronze object-key lineage against object inventory
        # and any Bronze table that exposes an object_key-like column.
        print("\n===== SILVER/GOLD OBJECT-KEY LINEAGE AUDIT =====", flush=True)
        object_key_parts = []
        for bronze_name in sorted(actual_bronze):
            bronze_df = table_df(spark, "bronze", bronze_name)
            for bronze_col in bronze_df.columns:
                if bronze_col.lower() in {
                    "object_key", "bronze_object_key", "source_object_key"
                }:
                    object_key_parts.append(
                        bronze_df.select(
                            F.col(bronze_col).cast("string").alias("_id")
                        ).filter(F.col("_id").isNotNull())
                    )
        bronze_object_keys = None
        if object_key_parts:
            bronze_object_keys = object_key_parts[0]
            for part in object_key_parts[1:]:
                bronze_object_keys = bronze_object_keys.unionByName(part)
            bronze_object_keys = bronze_object_keys.distinct()
            print(
                "BRONZE_UNIQUE_OBJECT_KEYS:",
                bronze_object_keys.count(),
                flush=True,
            )

        for namespace, names in (("silver", actual_silver), ("gold", actual_gold)):
            for table_name in sorted(names):
                df = table_df(spark, namespace, table_name)
                key_cols = [
                    c for c in df.columns
                    if "object_key" in c.lower()
                    and c.lower() not in {"object_key"}
                ]
                for column in key_cols:
                    missing = audit_reference(
                        df,
                        column,
                        bronze_object_keys,
                        f"{namespace}.{table_name}.{column} -> Bronze object keys",
                    )
                    if missing:
                        warnings.append(
                            f"{namespace}.{table_name}.{column}: {missing} object keys not found in Bronze object-key inventory; review if source uses an external object store"
                        )

        # 4. Check Silver table source hints where the hinted Bronze table
        # exists and has record_id. This is a stronger table-specific check.
        print("\n===== SILVER TO EXPECTED BRONZE SOURCE AUDIT =====", flush=True)

        for silver_name, bronze_candidates in SILVER_TO_BRONZE_HINTS.items():
            if silver_name not in actual_silver:
                continue

            present = [
                name for name in bronze_candidates
                if name in actual_bronze
            ]

            if not present:
                warnings.append(
                    f"Silver {silver_name}: no hinted Bronze source table found"
                )
                print(
                    "SOURCE_HINT_MISSING:",
                    silver_name,
                    bronze_candidates,
                    flush=True,
                )
                continue

            candidate_ids = union_ids(
                spark, "bronze", present, "record_id"
            )
            silver_df = table_df(spark, "silver", silver_name)

            lineage_cols = [
                c for c in silver_df.columns
                if "bronze" in c.lower() and "record_id" in c.lower()
            ]

            for column in lineage_cols:
                missing = audit_reference(
                    silver_df,
                    column,
                    candidate_ids,
                    f"silver.{silver_name}.{column} -> {present}",
                )
                if missing:
                    warnings.append(
                        f"Silver {silver_name}.{column}: {missing} IDs are not in hinted Bronze tables {present}; review whether the source is shared/combined"
                    )

        # 5. Gold -> Bronze lineage for all Gold record-id lineage columns.
        print("\n===== GOLD TO BRONZE LINEAGE AUDIT =====", flush=True)

        for name, df in sorted(gold_dfs.items()):
            lineage_cols = [
                c for c in df.columns
                if "bronze" in c.lower() and "record_id" in c.lower()
            ]

            for column in lineage_cols:
                missing = audit_reference(
                    df,
                    column,
                    bronze_ids,
                    f"gold.{name}.{column} -> all bronze record_id",
                )
                if missing:
                    failures.append(
                        f"gold.{name}.{column}: {missing} references not found in Bronze"
                    )

        # 6. Direct Gold -> Silver record-level lineage links.
        print("\n===== GOLD TO SILVER DIRECT LINEAGE AUDIT =====", flush=True)

        for gold_name, gold_col, silver_name, silver_col in GOLD_SILVER_LINKS:
            if gold_name not in actual_gold:
                continue
            if silver_name not in actual_silver:
                warnings.append(
                    f"Gold/Silver link source table missing: silver.{silver_name}"
                )
                continue

            gold_df = gold_dfs[gold_name]
            silver_df = table_df(spark, "silver", silver_name)

            if gold_col not in gold_df.columns or silver_col not in silver_df.columns:
                warnings.append(
                    f"Cannot test {gold_name}.{gold_col} -> {silver_name}.{silver_col}: column missing"
                )
                continue

            missing = audit_reference(
                gold_df,
                gold_col,
                silver_df.select(
                    F.col(silver_col).cast("string").alias("_id")
                ).filter(F.col("_id").isNotNull()).distinct(),
                f"gold.{gold_name}.{gold_col} -> silver.{silver_name}.{silver_col}",
            )

            if missing:
                failures.append(
                    f"Gold/Silver lineage broken: {gold_name}.{gold_col} has {missing} unmatched IDs"
                )

        # 7. Production migration consistency: the active 141-row production
        # table should have the same PK set as its 141-row staging table.
        print("\n===== PRODUCTION V1/V2 CONSISTENCY =====", flush=True)

        if (
            "fact_production" in actual_gold
            and "fact_production_v2_staging" in actual_gold
        ):
            active = gold_dfs["fact_production"]
            staging = gold_dfs["fact_production_v2_staging"]
            key = "production_fact_key"

            if key in active.columns and key in staging.columns:
                active_ids = active.select(key).distinct()
                staging_ids = staging.select(key).distinct()

                active_missing = active_ids.join(
                    staging_ids, key, "left_anti"
                ).count()
                staging_missing = staging_ids.join(
                    active_ids, key, "left_anti"
                ).count()

                print(
                    "PRODUCTION_ACTIVE_ROWS:",
                    active.count(),
                    "STAGING_ROWS:",
                    staging.count(),
                    "ACTIVE_NOT_IN_STAGING:",
                    active_missing,
                    "STAGING_NOT_IN_ACTIVE:",
                    staging_missing,
                    flush=True,
                )

                if active_missing or staging_missing:
                    failures.append(
                        "fact_production and fact_production_v2_staging have different production_fact_key sets"
                    )

        if (
            "fact_production_v1_backup" in actual_gold
            and "fact_production_v2_staging" in actual_gold
        ):
            backup = gold_dfs["fact_production_v1_backup"]
            staging = gold_dfs["fact_production_v2_staging"]

            if (
                "year_key" in backup.columns
                and "year_key" in staging.columns
                and "dataset_code" in staging.columns
            ):
                legacy = staging.filter(
                    F.col("dataset_code") == "LEGACY_VIETNAM_ANNUAL"
                )

                backup_years = backup.select("year_key").distinct()
                legacy_years = legacy.select("year_key").distinct()

                backup_not_in_legacy = backup_years.join(
                    legacy_years, "year_key", "left_anti"
                ).count()
                legacy_not_in_backup = legacy_years.join(
                    backup_years, "year_key", "left_anti"
                ).count()

                print(
                    "PRODUCTION_V1_BACKUP_ROWS:",
                    backup.count(),
                    "V2_LEGACY_ROWS:",
                    legacy.count(),
                    "BACKUP_YEARS_NOT_IN_V2:",
                    backup_not_in_legacy,
                    "V2_YEARS_NOT_IN_BACKUP:",
                    legacy_not_in_backup,
                    flush=True,
                )

                if backup_not_in_legacy or legacy_not_in_backup:
                    warnings.append(
                        "fact_production_v1_backup year coverage differs from V2 legacy subset; inspect before any migration"
                    )

        # 8. Final report.
        print("\n===== AUDIT SUMMARY =====", flush=True)
        print("GOLD_TABLES_CHECKED:", len(gold_dfs), flush=True)
        print("BRONZE_TABLES_CHECKED:", len(actual_bronze), flush=True)
        print("SILVER_TABLES_CHECKED:", len(actual_silver), flush=True)
        print("FAILURE_COUNT:", len(failures), flush=True)
        print("WARNING_COUNT:", len(warnings), flush=True)

        for item in failures:
            print("FAIL:", item, flush=True)

        for item in warnings:
            print("WARNING:", item, flush=True)

        if failures:
            print("GOLD_RELATIONSHIP_AUDIT_FAILED", flush=True)
            raise RuntimeError(
                f"Gold relationship audit found {len(failures)} failure(s)"
            )

        print("GOLD_RELATIONSHIP_AUDIT_PASSED", flush=True)
        print("READ_ONLY_NO_TABLE_WRITTEN", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
