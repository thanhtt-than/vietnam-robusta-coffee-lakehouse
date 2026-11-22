"""
Safe Iceberg Catalog recovery for Bronze and Silver.

Default behavior is READ-ONLY. It scans existing Iceberg metadata in MinIO,
compares table names with the Nessie Catalog, and writes a JSON audit report.

Only pass --apply after reviewing the dry-run report. The script never deletes
or overwrites MinIO objects, never drops tables, and never re-runs ETL.

Run from the repository root:
  .\run_spark.ps1 -Script "src/iceberg/recover_bronze_silver_catalog.py"

Apply only after reviewing the report:
  .\run_spark.ps1 -Script "src/iceberg/recover_bronze_silver_catalog.py" -ScriptArgs "--apply"
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from urllib.parse import urlparse

from minio_direct import MinioDirect, spark_session


UUID_SUFFIX = re.compile(
    r"^(?P<table>.+)_(?P<uuid>[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$"
)


def object_key_from_s3_uri(uri: str, expected_bucket: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or parsed.netloc != expected_bucket:
        raise ValueError(f"Unexpected S3 URI or bucket: {uri}")
    return parsed.path.lstrip("/")


def get_catalog_tables(spark, layer: str) -> set[str]:
    try:
        rows = spark.sql(f"SHOW TABLES IN lakehouse.{layer}").collect()
        return {row["tableName"].lower() for row in rows}
    except Exception as exc:
        # A missing namespace is treated as an empty namespace in audit mode.
        message = str(exc).lower()
        if "namespace" in message and (
            "not found" in message or "does not exist" in message
        ):
            return set()
        raise


def inspect_metadata(client, layer: str, folder: str, metadata_key: str,
                     all_layer_keys: set[str]) -> dict:
    raw = client.read(metadata_key)
    metadata = json.loads(raw.decode("utf-8"))

    match = UUID_SUFFIX.match(folder)
    if not match:
        return {
            "layer": layer,
            "folder": folder,
            "metadata_key": metadata_key,
            "status": "UNRECOGNIZED_FOLDER_NAME",
            "reason": "Folder does not end with a standard Iceberg table UUID",
        }

    table_name = match.group("table").lower()
    folder_uuid = match.group("uuid").lower()
    table_uuid = str(metadata.get("table-uuid") or "").lower()
    snapshot_id = metadata.get("current-snapshot-id")
    snapshots = metadata.get("snapshots") or []
    current_snapshot = next(
        (item for item in snapshots if item.get("snapshot-id") == snapshot_id),
        None,
    )

    result = {
        "layer": layer,
        "table_name": table_name,
        "folder": folder,
        "folder_uuid": folder_uuid,
        "table_uuid": table_uuid,
        "metadata_key": metadata_key,
        "last_updated_ms": metadata.get("last-updated-ms", 0),
        "format_version": metadata.get("format-version"),
        "current_snapshot_id": snapshot_id,
        "snapshot_count": len(snapshots),
        "location": metadata.get("location"),
        "status": "CANDIDATE",
        "reason": "",
    }

    if not table_uuid:
        result.update(status="INVALID_METADATA", reason="Missing table-uuid")
        return result

    # if table_uuid != folder_uuid:
    #     result.update(
    #         status="INVALID_METADATA",
    #         reason="Folder UUID does not match metadata table-uuid",
    #     )
    #     return result

    expected_location = (
        f"s3://{client.bucket}/iceberg-warehouse/{layer}/{folder}"
    )
    location = str(metadata.get("location") or "").rstrip("/")
    if location != expected_location:
        result.update(
            status="LOCATION_MISMATCH",
            reason=f"Expected {expected_location}, found {location or '<empty>'}",
        )
        return result

    if snapshot_id in (None, -1):
        result.update(
            status="NO_ACTIVE_SNAPSHOT",
            reason="Metadata has no active snapshot; do not auto-register",
        )
        return result

    if current_snapshot is None:
        result.update(
            status="INVALID_METADATA",
            reason="current-snapshot-id is not present in snapshots[]",
        )
        return result

    manifest_uri = current_snapshot.get("manifest-list")
    if not manifest_uri:
        result.update(
            status="INVALID_METADATA",
            reason="Current snapshot has no manifest-list URI",
        )
        return result

    try:
        manifest_key = object_key_from_s3_uri(manifest_uri, client.bucket)
    except ValueError as exc:
        result.update(status="INVALID_METADATA", reason=str(exc))
        return result

    result["manifest_list_uri"] = manifest_uri
    result["manifest_list_key"] = manifest_key
    if manifest_key not in all_layer_keys:
        result.update(
            status="MISSING_MANIFEST_LIST",
            reason="Current snapshot manifest-list object is absent from MinIO",
        )
        return result

    result["status"] = "VALID_CANDIDATE"
    return result


def scan_layer(client, layer: str) -> list[dict]:
    prefix = f"iceberg-warehouse/{layer}/"
    keys = list(client.list_keys(prefix))
    key_set = set(keys)

    folders = sorted({
        key[len(prefix):].split("/", 1)[0]
        for key in keys
        if key.startswith(prefix) and "/" in key[len(prefix):]
    })

    candidates = []
    for folder in folders:
        folder_prefix = f"{prefix}{folder}/"
        metadata_keys = sorted(
            key for key in keys
            if key.startswith(folder_prefix)
            and "/metadata/" in key
            and key.endswith(".metadata.json")
        )
        if not metadata_keys:
            continue

        inspected = [
            inspect_metadata(client, layer, folder, key, key_set)
            for key in metadata_keys
        ]

        valid = [item for item in inspected if item["status"] == "VALID_CANDIDATE"]
        if not valid:
            candidates.extend(inspected)
            continue

        # Multiple metadata JSON files in one UUID folder are normal Iceberg
        # history. Select the highest last-updated-ms only when it is unique.
        newest_timestamp = max(item.get("last_updated_ms", 0) for item in valid)
        newest = [
            item for item in valid
            if item.get("last_updated_ms", 0) == newest_timestamp
        ]
        if len(newest) != 1:
            for item in newest:
                item.update(
                    status="AMBIGUOUS_METADATA_VERSION",
                    reason="More than one valid metadata file has the same newest timestamp",
                )
            candidates.extend(inspected)
        else:
            selected = newest[0]
            selected["metadata_versions_in_folder"] = len(metadata_keys)
            candidates.append(selected)

    return candidates


def choose_unambiguous_tables(candidates: list[dict]) -> tuple[list[dict], list[dict]]:
    by_name = defaultdict(list)
    for item in candidates:
        if item.get("status") == "VALID_CANDIDATE":
            by_name[item["table_name"]].append(item)

    safe = []
    ambiguous = []
    for name, items in sorted(by_name.items()):
        # More than one folder/UUID for the same logical table is a merge
        # collision. Do not guess which branch is authoritative.
        if len(items) != 1:
            for item in items:
                item.update(
                    status="AMBIGUOUS_TABLE_UUID",
                    reason="Multiple valid MinIO folders/UUIDs share this table name",
                )
                ambiguous.append(item)
        else:
            safe.append(items[0])

    return safe, ambiguous


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "both"],
        default="both",
        help="Which Iceberg namespaces to inspect",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Register unambiguous missing tables in Nessie. Default is read-only.",
    )
    args = parser.parse_args()

    layers = ["bronze", "silver"] if args.layer == "both" else [args.layer]
    client = MinioDirect()
    spark = spark_session("TLCN-Bronze-Silver-Catalog-Recovery")

    report = {
        "mode": "APPLY" if args.apply else "DRY_RUN_READ_ONLY",
        "layers": {},
        "summary": {},
    }

    try:
        total_registered = 0
        total_already_present = 0
        total_ambiguous = 0
        total_invalid = 0
        total_errors = 0

        for layer in layers:
            print(f"\n========== {layer.upper()} AUDIT ==========", flush=True)
            candidates = scan_layer(client, layer)
            safe, ambiguous = choose_unambiguous_tables(candidates)
            catalog_tables = get_catalog_tables(spark, layer)

            layer_result = {
                "catalog_tables_before": sorted(catalog_tables),
                "candidates": candidates,
                "safe_candidates": [],
                "already_registered": [],
                "ambiguous": ambiguous,
                "invalid_or_incomplete": [],
                "registered": [],
                "errors": [],
            }

            for item in candidates:
                if item.get("status") not in (
                    "VALID_CANDIDATE", "AMBIGUOUS_TABLE_UUID"
                ):
                    layer_result["invalid_or_incomplete"].append(item)

            for item in safe:
                name = item["table_name"]
                full_name = f"lakehouse.{layer}.{name}"
                metadata_uri = f"s3://{client.bucket}/{item['metadata_key']}"

                if name in catalog_tables:
                    print(f"ALREADY_REGISTERED: {full_name}", flush=True)
                    layer_result["already_registered"].append(item)
                    total_already_present += 1
                    continue

                safe_item = dict(item)
                safe_item["table_identifier"] = full_name
                safe_item["metadata_uri"] = metadata_uri
                layer_result["safe_candidates"].append(safe_item)

                print(f"SAFE_TO_REGISTER: {full_name}", flush=True)
                print(f"  UUID: {item['table_uuid']}", flush=True)
                print(f"  SNAPSHOT: {item['current_snapshot_id']}", flush=True)
                print(f"  METADATA: {metadata_uri}", flush=True)

                if not args.apply:
                    continue

                try:
                    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{layer}")
                    result = spark.sql(f"""
                        CALL lakehouse.system.register_table(
                            table => '{layer}.{name}',
                            metadata_file => '{metadata_uri}'
                        )
                    """)
                    result.show(truncate=False)

                    refreshed = get_catalog_tables(spark, layer)
                    if name not in refreshed:
                        raise RuntimeError("register_table returned but table is absent from SHOW TABLES")

                    row_count = spark.table(full_name).count()
                    registered_item = dict(safe_item)
                    registered_item["post_registration_row_count"] = row_count
                    registered_item["post_registration_status"] = (
                        "PASS" if row_count > 0 else "ZERO_ROWS_REVIEW"
                    )
                    layer_result["registered"].append(registered_item)
                    total_registered += 1

                    print(f"REGISTERED: {full_name}", flush=True)
                    print(f"POST_REGISTER_ROW_COUNT: {row_count}", flush=True)
                    if row_count == 0:
                        print(
                            f"WARNING_ZERO_ROWS_REVIEW: {full_name}",
                            flush=True,
                        )

                except Exception as exc:
                    error = {
                        "table": full_name,
                        "metadata_uri": metadata_uri,
                        "error": repr(exc),
                    }
                    layer_result["errors"].append(error)
                    total_errors += 1
                    print(f"REGISTER_ERROR: {full_name}: {exc!r}", flush=True)

            total_ambiguous += len(ambiguous)
            total_invalid += len(layer_result["invalid_or_incomplete"])
            layer_result["catalog_tables_after"] = sorted(
                get_catalog_tables(spark, layer)
            )
            report["layers"][layer] = layer_result

        report["summary"] = {
            "registered": total_registered,
            "already_registered": total_already_present,
            "ambiguous": total_ambiguous,
            "invalid_or_incomplete": total_invalid,
            "errors": total_errors,
        }

        os.makedirs("/workspace/output/iceberg_recovery", exist_ok=True)
        report_path = (
            "/workspace/output/iceberg_recovery/"
            "bronze_silver_catalog_recovery_report.json"
        )
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)

        print("\n========== RECOVERY SUMMARY ==========", flush=True)
        print("MODE:", report["mode"], flush=True)
        for key, value in report["summary"].items():
            print(f"{key.upper()}: {value}", flush=True)
        print("REPORT:", report_path, flush=True)

        if args.apply:
            print("CATALOG_RECOVERY_APPLY_FINISHED", flush=True)
        else:
            print("CATALOG_RECOVERY_DRY_RUN_FINISHED", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
