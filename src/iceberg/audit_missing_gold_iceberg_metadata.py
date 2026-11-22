import json
from urllib.parse import urlparse
from minio_direct import MinioDirect

client = MinioDirect()

tables = [
    "fact_exchange_rate_d5c7418a-0025-4c77-b27a-b23b7c54a7af",
    "fact_global_coffee_exports_4ed8da40-14cc-49ab-8047-8f5d99350806",
    "dim_coffee_exporter_6bceee17-b164-4d00-0000-000000000000",
    "dim_currency_pair_c370e5a2-7f5d-47d1-0000-000000000000",
    "fact_coffee_trade_monthly_49997c9e-9471-4f33-98f3-51dcd9c32372",
]

base = "iceberg-warehouse/gold/"

for table in tables:
    prefix = base + table + "/"

    print("\n" + "=" * 90)
    print("TABLE:", table)

    try:
        keys = list(client.list_keys(prefix))
        key_set = set(keys)

        metadata_keys = [
            k for k in keys
            if k.endswith(".metadata.json")
        ]

        print("TOTAL_OBJECTS:", len(keys))
        print("METADATA_FILES:", len(metadata_keys))

        if not metadata_keys:
            print("NO_METADATA_FOUND")
            continue

        uuids = set()

        for key in metadata_keys:
            raw = client.read(key)
            metadata = json.loads(raw.decode("utf-8"))

            table_uuid = metadata.get("table-uuid")
            uuids.add(table_uuid)

            snapshot_id = metadata.get("current-snapshot-id")
            snapshots = metadata.get("snapshots", [])

            current = next(
                (s for s in snapshots
                 if s.get("snapshot-id") == snapshot_id),
                None
            )

            manifest = (
                current.get("manifest-list")
                if current else None
            )

            manifest_exists = None

            if manifest:
                parsed = urlparse(manifest)
                manifest_key = parsed.path.lstrip("/")
                if manifest_key.startswith(client.bucket + "/"):
                    manifest_key = manifest_key[len(client.bucket) + 1:]
                manifest_exists = manifest_key in key_set

            print("\nMETADATA:", key)
            print("TABLE_UUID:", table_uuid)
            print("FORMAT_VERSION:", metadata.get("format-version"))
            print("LAST_UPDATED_MS:", metadata.get("last-updated-ms"))
            print("LAST_SEQUENCE_NUMBER:", metadata.get("last-sequence-number"))
            print("CURRENT_SNAPSHOT_ID:", snapshot_id)
            print("SNAPSHOT_COUNT:", len(snapshots))
            print("MANIFEST_LIST:", manifest)
            print("MANIFEST_EXISTS:", manifest_exists)

            print("METADATA_LOG:")
            for item in metadata.get("metadata-log", []):
                print(json.dumps(item, ensure_ascii=False))

        print("\nUUID_VARIANTS:", len(uuids))

        if len(uuids) > 1:
            print("WARNING: DIFFERENT_TABLE_UUIDS")
        else:
            print("TABLE_UUID_CONSISTENT")

    except Exception as exc:
        print("AUDIT_ERROR:", repr(exc))

print("\nGOLD_METADATA_AUDIT_COMPLETED")
