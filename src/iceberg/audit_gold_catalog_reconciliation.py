from minio_direct import MinioDirect, spark_session
import json

client = MinioDirect()
spark = spark_session("TLCN-Gold-Catalog-Reconciliation")

try:
    prefix = "iceberg-warehouse/gold/"
    keys = list(client.list_keys(prefix))

    folders = sorted(set(
        key[len(prefix):].split("/")[0]
        for key in keys
        if "/" in key[len(prefix):]
    ))

    catalog_rows = spark.sql(
        "SHOW TABLES IN lakehouse.gold"
    ).collect()

    catalog_names = {
        row.tableName.lower()
        for row in catalog_rows
    }

    print("\n========== NESSIE CATALOG ==========")
    print("CATALOG_TABLE_COUNT:", len(catalog_names))

    print("\n========== MINIO GOLD AUDIT ==========")

    missing = []
    registered = []
    invalid = []

    for folder in folders:
        if not folder.startswith(("dim_", "fact_")):
            continue

        table_name = folder.rsplit("_", 1)[0]
        table_prefix = prefix + folder + "/"

        metadata_keys = [
            key for key in keys
            if key.startswith(table_prefix)
            and key.endswith(".metadata.json")
        ]

        versions = []

        for key in metadata_keys:
            metadata = json.loads(
                client.read(key).decode("utf-8")
            )

            snapshot = metadata.get("current-snapshot-id")

            if snapshot not in (-1, None):
                versions.append({
                    "key": key,
                    "metadata": metadata
                })

        print("\nTABLE:", table_name)
        print("FOLDER:", folder)
        print("METADATA_VERSIONS:", len(metadata_keys))

        if not versions:
            print("STATUS: NO_ACTIVE_SNAPSHOT")
            invalid.append(table_name)
            continue

        latest = max(
            versions,
            key=lambda x: x["metadata"].get(
                "last-updated-ms", 0
            )
        )

        metadata = latest["metadata"]
        location = metadata.get("location")

        print("UUID:", metadata.get("table-uuid"))
        print("SNAPSHOT:", metadata.get("current-snapshot-id"))
        print("LOCATION:", location)
        print("METADATA_KEY:", latest["key"])

        expected_location = (
            f"s3://{client.bucket}/{table_prefix.rstrip('/')}"
        )

        if location and location.rstrip("/") != expected_location:
            print("WARNING: LOCATION_MISMATCH")
            invalid.append(table_name)
            continue

        if table_name.lower() in catalog_names:
            print("CATALOG_STATUS: REGISTERED")
            registered.append(table_name)
        else:
            print("CATALOG_STATUS: MISSING")
            missing.append({
                "name": table_name,
                "metadata": latest["key"]
            })

    print("\n========== FINAL SUMMARY ==========")
    print("MINIO_REGISTERED:", len(registered))
    print("MISSING_IN_NESSIE:", len(missing))
    print("INVALID_OR_NO_SNAPSHOT:", len(invalid))

    for item in missing:
        print("MISSING_TABLE:", item["name"])
        print("METADATA_FILE:", item["metadata"])

    for name in invalid:
        print("INVALID_TABLE:", name)

    print("\nGOLD_CATALOG_RECONCILIATION_COMPLETED")

finally:
    spark.stop()
