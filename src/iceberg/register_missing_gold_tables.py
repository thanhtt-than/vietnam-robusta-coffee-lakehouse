
import json

from minio_direct import MinioDirect, spark_session

TABLES = [
    (
        "dim_coffee_exporter",
        "iceberg-warehouse/gold/dim_coffee_exporter_6bdeee17-b164-4d5e-9a78-b4b7ed06a59e/metadata/00000-f71b6753-e66b-4a9f-9b7a-e589deb1513c.metadata.json"
    ),
    (
        "dim_currency_pair",
        "iceberg-warehouse/gold/dim_currency_pair_c370e5a2-f75d-47d1-a722-9d17c9130e8e/metadata/00000-db589b73-01e0-43ee-8d08-1b14b88bb192.metadata.json"
    ),
    (
        "fact_coffee_trade_monthly",
        "iceberg-warehouse/gold/fact_coffee_trade_monthly_49997c9e-9471-4f33-98f3-51dcd9c32372/metadata/00000-2175587a-18e1-4b00-9283-f2327cbe73cd.metadata.json"
    ),
    (
        "fact_exchange_rate",
        "iceberg-warehouse/gold/fact_exchange_rate_d5c7418a-0025-4c77-b27a-b23b7c54a7af/metadata/00000-79de4375-ae3a-4449-b3bb-5f099f6e8561.metadata.json"
    ),
    (
        "fact_global_coffee_exports",
        "iceberg-warehouse/gold/fact_global_coffee_exports_4ed8da40-14cc-49ab-8047-8f5d99350806/metadata/00000-7dab2ac4-544a-426a-a59c-f633b18f6cef.metadata.json"
    )
]


def get_tables(spark):
    rows = spark.sql(
        "SHOW TABLES IN lakehouse.gold"
    ).collect()

    return {r["tableName"].lower() for r in rows}


def main():
    client = MinioDirect()
    spark = spark_session(
        "TLCN-Register-Missing-Gold-Tables"
    )

    try:
        existing = get_tables(spark)

        print("GOLD_TABLES_BEFORE:", len(existing))

        for name, metadata_key in TABLES:

            if name.lower() in existing:
                print("ALREADY_REGISTERED:", name)
                continue

            # Verify metadata from MinIO
            metadata = json.loads(
                client.read(metadata_key).decode("utf-8")
            )

            snapshot = metadata.get("current-snapshot-id")
            location = metadata.get("location")
            table_uuid = metadata.get("table-uuid")

            if snapshot in (None, -1):
                raise RuntimeError(
                    f"{name}: No active snapshot"
                )

            if not table_uuid:
                raise RuntimeError(
                    f"{name}: Missing table UUID"
                )

            expected_prefix = (
                f"s3://{client.bucket}/iceberg-warehouse/gold/"
            )

            if not location or not location.startswith(
                expected_prefix
            ):
                raise RuntimeError(
                    f"{name}: Invalid location: {location}"
                )

            metadata_uri = (
                f"s3://{client.bucket}/{metadata_key}"
            )

            print("\nREGISTERING:", name)
            print("UUID:", table_uuid)
            print("SNAPSHOT:", snapshot)

            # Register existing metadata into Nessie
            result = spark.sql(f"""
                CALL lakehouse.system.register_table(
                    table => 'gold.{name}',
                    metadata_file => '{metadata_uri}'
                )
            """)

            result.show(truncate=False)

            # Verify catalog registration
            existing = get_tables(spark)

            if name.lower() not in existing:
                raise RuntimeError(
                    f"{name}: Registration failed"
                )

            # Verify actual data accessibility
            table = spark.table(
                f"lakehouse.gold.{name}"
            )

            count = table.count()

            if count <= 0:
                raise RuntimeError(
                    f"{name}: No accessible records"
                )

            print("REGISTERED:", name)
            print("TOTAL_ROWS:", count)

        final_tables = get_tables(spark)

        missing = [
            name for name, _ in TABLES
            if name.lower() not in final_tables
        ]

        print("\nGOLD_TABLES_AFTER:", len(final_tables))
        print("REMAINING_MISSING:", len(missing))

        if missing:
            raise RuntimeError(
                f"Still missing tables: {missing}"
            )

        print(
            "ALL_FIVE_GOLD_TABLES_REGISTERED",
            flush=True
        )

        print(
            "GOLD_CATALOG_REPAIR_PASSED",
            flush=True
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
