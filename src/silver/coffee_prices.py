"""Silver Coffee Prices pipeline — Bronze JSON to validated Silver Parquet."""

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = PROJECT_ROOT / "reports/silver/coffee_prices_data_contract_v1.json"
DQ_RULES_PATH = PROJECT_ROOT / "reports/silver/silver_dq_rules_v1.json"


def load_configuration():
    with CONTRACT_PATH.open("r", encoding="utf-8-sig") as f:
        contract = json.load(f)

    with DQ_RULES_PATH.open("r", encoding="utf-8-sig") as f:
        dq_rules = json.load(f)

    assert contract["table"] == "silver/coffee_prices"
    assert contract["format"] == "parquet"
    assert contract["business_key"] == dq_rules["tables"]["coffee_prices"]["business_key"]

    return contract, dq_rules



def read_bronze_records(client, bucket, prefix):
    """Read raw BBW JSON and preserve deterministic Bronze lineage."""
    records = []
    raw_files = 0
    empty_files = 0

    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        key = obj.object_name

        if not key.endswith(".json") or key.endswith("metadata.json"):
            continue

        response = client.get_object(bucket, key)
        try:
            payload = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()

        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ValueError(f"Invalid Bronze JSON schema: {key}")

        raw_files += 1
        data = payload["data"]

        if not data:
            empty_files += 1

        for index, record in enumerate(data):
            if not isinstance(record, dict):
                raise ValueError(f"Invalid record at {key}, index={index}")

            enriched = dict(record)
            enriched["bronze_object_key"] = key
            enriched["bronze_row_index"] = index
            records.append(enriched)

    return records, {
        "raw_files": raw_files,
        "empty_files": empty_files,
        "total_rows": len(records),
    }


def normalize_coffee_record(record):
    """Normalize one Bronze coffee record to Silver contract types."""
    from datetime import date, datetime, timezone
    from decimal import Decimal, InvalidOperation

    def utc_timestamp(value):
        if value is None or value == "":
            return None

        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"Timestamp missing timezone: {value}")

        return parsed.astimezone(timezone.utc)

    def nested_value(parent, child):
        obj = record.get(parent)
        return obj.get(child) if isinstance(obj, dict) else None

    raw_date = record.get("quote_date_vn")
    quote_date = (
        date.fromisoformat(str(raw_date))
        if raw_date is not None and raw_date != ""
        else None
    )

    raw_price = record.get("price")
    try:
        price = (
            Decimal(str(raw_price)).quantize(Decimal("0.01"))
            if raw_price is not None and raw_price != ""
            else None
        )
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid price: {raw_price}") from exc

    return {
        "product": record.get("product"),
        "region": record.get("region"),
        "series_code": record.get("series_code"),
        "quote_date": quote_date,
        "quoted_at": utc_timestamp(record.get("quoted_at")),
        "price": price,
        "currency": record.get("currency"),
        "unit": record.get("unit"),
        "provider": nested_value("meta", "provider"),
        "market": nested_value("meta", "market"),
        "price_change_raw": nested_value("meta", "change"),
        "source_code": nested_value("source", "code"),
        "source_name": nested_value("source", "name"),
        "updated_at": utc_timestamp(record.get("updated_at")),
        "bronze_object_key": record.get("bronze_object_key"),
        "bronze_row_index": record.get("bronze_row_index"),
    }


def validate_coffee_record(row):
    """Return DQ violation codes for a normalized coffee record."""
    from datetime import date, datetime, timezone
    from decimal import Decimal

    errors = []

    for field in ("series_code", "quote_date", "source_code", "price"):
        value = row.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            errors.append(f"missing_required:{field}")

    if row.get("product") != "COFFEE":
        errors.append("invalid_product")

    if row.get("currency") != "VND":
        errors.append("invalid_currency")

    if row.get("unit") != "VND/KG":
        errors.append("invalid_unit")

    price = row.get("price")
    if price is not None:
        if not isinstance(price, Decimal):
            errors.append("invalid_price_type")
        elif not price.is_finite() or price <= 0:
            errors.append("price_not_positive")

    if not isinstance(row.get("quote_date"), date) or isinstance(
        row.get("quote_date"), datetime
    ):
        errors.append("invalid_quote_date")

    for field in ("quoted_at", "updated_at"):
        value = row.get(field)
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
            or value.utcoffset().total_seconds() != 0
        ):
            errors.append(f"invalid_utc_timestamp:{field}")

    object_key = row.get("bronze_object_key")
    row_index = row.get("bronze_row_index")

    if not isinstance(object_key, str) or not object_key.strip():
        errors.append("missing_bronze_object_key")

    if type(row_index) is not int or row_index < 0:
        errors.append("invalid_bronze_row_index")

    return errors


def deduplicate_coffee_records(rows):
    """Deduplicate by latest updated_at with deterministic Bronze lineage."""
    from collections import Counter

    business_key_fields = ("series_code", "quote_date", "source_code")

    def business_key(row):
        return tuple(row[field] for field in business_key_fields)

    def priority(row):
        return (
            -row["updated_at"].timestamp(),
            row["bronze_object_key"],
            row["bronze_row_index"],
        )

    winners = {}
    counts = Counter()

    for row in rows:
        key = business_key(row)
        counts[key] += 1

        if key not in winners or priority(row) < priority(winners[key]):
            winners[key] = row

    result = [winners[key] for key in sorted(winners)]

    metrics = {
        "input_rows": len(rows),
        "output_rows": len(result),
        "removed_duplicates": len(rows) - len(result),
        "duplicate_key_groups": sum(count > 1 for count in counts.values()),
    }

    return result, metrics


def get_coffee_silver_schema():
    """Return the explicit 16-column Coffee Prices Silver schema."""
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DateType,
        TimestampType,
        DecimalType,
    )

    return StructType([
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("series_code", StringType(), False),
        StructField("quote_date", DateType(), False),
        StructField("quoted_at", TimestampType(), True),
        StructField("price", DecimalType(18, 2), False),
        StructField("currency", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("market", StringType(), True),
        StructField("price_change_raw", StringType(), True),
        StructField("source_code", StringType(), False),
        StructField("source_name", StringType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("bronze_object_key", StringType(), False),
        StructField("silver_processed_at", TimestampType(), False),
    ])


def upload_parquet_to_minio(client, bucket, local_parquet_dir, target_prefix):
    """Upload Spark Parquet files to an empty MinIO target prefix."""
    from pathlib import Path

    directory = Path(local_parquet_dir)

    if not directory.is_dir():
        raise ValueError(f"Parquet directory not found: {directory}")

    parquet_files = sorted(directory.glob("*.parquet"))

    if not parquet_files:
        raise ValueError(f"No Parquet files found: {directory}")

    prefix = target_prefix.strip("/") + "/"

    existing = next(
        client.list_objects(bucket, prefix=prefix, recursive=True),
        None,
    )

    if existing is not None:
        raise FileExistsError(
            f"MinIO target prefix is not empty: {prefix}"
        )

    uploaded_keys = []

    for parquet_file in parquet_files:
        object_key = prefix + parquet_file.name

        client.fput_object(
            bucket,
            object_key,
            str(parquet_file),
            content_type="application/octet-stream",
        )

        uploaded_keys.append(object_key)

    return uploaded_keys


def upload_verified_parquet(client, bucket, local_parquet_dir, target_prefix):
    """Upload Parquet to an empty prefix and verify downloaded SHA-256."""
    import hashlib
    import tempfile
    from pathlib import Path

    directory = Path(local_parquet_dir)
    parquet_files = sorted(directory.glob("*.parquet"))

    if not parquet_files:
        raise ValueError(f"No Parquet files found: {directory}")

    prefix = target_prefix.strip("/") + "/"

    if next(client.list_objects(bucket, prefix=prefix, recursive=True), None):
        raise FileExistsError(f"Target prefix is not empty: {prefix}")

    def sha256(file_path):
        digest = hashlib.sha256()
        with Path(file_path).open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    manifest = []

    with tempfile.TemporaryDirectory(prefix="tlcn_verify_upload_") as tmp:
        for parquet_file in parquet_files:
            object_key = prefix + parquet_file.name
            expected_hash = sha256(parquet_file)

            client.fput_object(
                bucket,
                object_key,
                str(parquet_file),
                content_type="application/octet-stream",
            )

            downloaded = Path(tmp) / parquet_file.name
            client.fget_object(bucket, object_key, str(downloaded))
            actual_hash = sha256(downloaded)

            if expected_hash != actual_hash:
                raise RuntimeError(
                    f"SHA256 verification failed: {object_key}"
                )

            manifest.append({
                "object_key": object_key,
                "size_bytes": parquet_file.stat().st_size,
                "sha256": expected_hash,
            })

    return manifest


def build_coffee_run_report(
    bronze_metrics,
    valid_count,
    quarantine_rows,
    dedup_metrics,
    parquet_manifest,
    run_id,
):
    """Build an auditable Silver Coffee Prices run report."""
    from collections import Counter
    from datetime import datetime, timezone

    error_counts = Counter()

    for item in quarantine_rows:
        error_counts.update(item.get("errors", []))

    report = {
        "pipeline": "silver_coffee_prices",
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "bronze/coffee_prices/source=bbw_coffee/",
        "target": "silver/coffee_prices/",
        "bronze": {
            "raw_files": bronze_metrics["raw_files"],
            "empty_files": bronze_metrics["empty_files"],
            "total_rows": bronze_metrics["total_rows"],
        },
        "data_quality": {
            "valid_rows_before_dedup": valid_count,
            "quarantine_rows": len(quarantine_rows),
            "error_counts": dict(sorted(error_counts.items())),
        },
        "deduplication": dict(dedup_metrics),
        "silver": {
            "output_rows": dedup_metrics["output_rows"],
            "parquet_files": len(parquet_manifest),
            "parquet_manifest": parquet_manifest,
        },
    }

    if (
        valid_count + len(quarantine_rows)
        != bronze_metrics["total_rows"]
    ):
        raise ValueError("DQ row accounting mismatch")

    if (
        dedup_metrics["output_rows"]
        + dedup_metrics["removed_duplicates"]
        != valid_count
    ):
        raise ValueError("Dedup row accounting mismatch")

    return report


def compute_coffee_run_id(client, bucket, prefix):
    """Compute a deterministic run ID from Bronze object keys and bytes."""
    import hashlib

    objects = sorted(
        (
            obj.object_name
            for obj in client.list_objects(
                bucket, prefix=prefix, recursive=True
            )
            if obj.object_name.endswith(".json")
            and not obj.object_name.endswith("metadata.json")
        )
    )

    if not objects:
        raise ValueError(f"No Bronze JSON files found: {prefix}")

    digest = hashlib.sha256()

    for key in objects:
        response = client.get_object(bucket, key)

        try:
            content_hash = hashlib.sha256()

            for chunk in response.stream(1024 * 1024):
                content_hash.update(chunk)
        finally:
            response.close()
            response.release_conn()

        key_bytes = key.encode("utf-8")
        digest.update(len(key_bytes).to_bytes(8, "big"))
        digest.update(key_bytes)
        digest.update(content_hash.digest())

    return "coffee_" + digest.hexdigest()[:24]


def get_coffee_run_status(client, bucket, run_id):
    """Inspect a Silver Coffee run without modifying MinIO."""
    import json
    import re

    if not re.fullmatch(r"coffee_[0-9a-f]{24}", run_id):
        raise ValueError(f"Invalid Coffee run ID: {run_id}")

    prefix = f"silver/coffee_prices/runs/{run_id}/"
    manifest_key = prefix + "manifest.json"

    object_keys = sorted(
        obj.object_name
        for obj in client.list_objects(
            bucket, prefix=prefix, recursive=True
        )
    )

    if not object_keys:
        return {
            "run_id": run_id,
            "status": "NEW",
            "prefix": prefix,
            "object_count": 0,
            "reason": "No objects found",
        }

    if manifest_key not in object_keys:
        return {
            "run_id": run_id,
            "status": "INCOMPLETE",
            "prefix": prefix,
            "object_count": len(object_keys),
            "reason": "Completion manifest missing",
        }

    response = client.get_object(bucket, manifest_key)

    try:
        manifest = json.loads(response.read().decode("utf-8-sig"))
    except (ValueError, UnicodeError, TypeError):
        return {
            "run_id": run_id,
            "status": "INCOMPLETE",
            "prefix": prefix,
            "object_count": len(object_keys),
            "reason": "Invalid completion manifest",
        }
    finally:
        response.close()
        response.release_conn()

    if not isinstance(manifest, dict):
        return {
            "run_id": run_id,
            "status": "INCOMPLETE",
            "prefix": prefix,
            "object_count": len(object_keys),
            "reason": "Manifest must be a JSON object",
        }

    parquet_manifest = manifest.get("parquet_manifest")

    if (
        manifest.get("run_id") != run_id
        or manifest.get("status") != "COMPLETED"
        or not isinstance(parquet_manifest, list)
        or not parquet_manifest
    ):
        return {
            "run_id": run_id,
            "status": "INCOMPLETE",
            "prefix": prefix,
            "object_count": len(object_keys),
            "reason": "Manifest metadata incomplete",
        }

    expected_keys = set()

    for entry in parquet_manifest:
        if not isinstance(entry, dict):
            break

        key = entry.get("object_key")

        if (
            not isinstance(key, str)
            or not key.startswith(prefix)
            or not key.endswith(".parquet")
        ):
            break

        expected_keys.add(key)
    else:
        actual_parquet_keys = {
            key for key in object_keys
            if key.endswith(".parquet")
        }

        if (
            len(expected_keys) == len(parquet_manifest)
            and expected_keys == actual_parquet_keys
        ):
            return {
                "run_id": run_id,
                "status": "COMPLETED",
                "prefix": prefix,
                "object_count": len(object_keys),
                "reason": "Completion manifest and Parquet objects present",
            }

    return {
        "run_id": run_id,
        "status": "INCOMPLETE",
        "prefix": prefix,
        "object_count": len(object_keys),
        "reason": "Manifest Parquet inventory mismatch",
    }


def compute_coffee_versioned_run_id(
    client,
    bucket,
    bronze_prefix,
    contract_path="reports/silver/coffee_prices_data_contract_v1.json",
    dq_path="reports/silver/silver_dq_rules_v1.json",
):
    """Bind the run ID to Bronze input and Silver processing definitions."""
    import hashlib
    from pathlib import Path

    bronze_run_id = compute_coffee_run_id(
        client, bucket, bronze_prefix
    )

    digest = hashlib.sha256()
    digest.update(b"tlcn-coffee-silver-run-v2")

    inputs = [
        ("bronze_fingerprint", bronze_run_id.encode("utf-8")),
        ("contract", Path(contract_path).read_bytes()),
        ("dq_rules", Path(dq_path).read_bytes()),
        ("pipeline_code", Path(__file__).read_bytes()),
    ]

    for name, content in inputs:
        name_bytes = name.encode("utf-8")
        digest.update(len(name_bytes).to_bytes(8, "big"))
        digest.update(name_bytes)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)

    return "coffee_" + digest.hexdigest()[:24]


def publish_coffee_run_manifest(
    client,
    bucket,
    run_id,
    parquet_manifest,
    dq_report,
):
    """Verify uploaded Parquet and publish a completion manifest last."""
    import hashlib
    import json
    import re
    import tempfile
    from datetime import datetime, timezone
    from io import BytesIO
    from pathlib import Path

    if not re.fullmatch(r"coffee_[0-9a-f]{24}", run_id):
        raise ValueError("Invalid Coffee run ID")

    if not parquet_manifest:
        raise ValueError("Parquet manifest cannot be empty")

    if not isinstance(dq_report, dict):
        raise ValueError("DQ report must be a dictionary")

    if dq_report.get("run_id") != run_id:
        raise ValueError("DQ report Run ID mismatch")

    prefix = f"silver/coffee_prices/runs/{run_id}/"
    manifest_key = prefix + "manifest.json"

    existing_keys = {
        obj.object_name
        for obj in client.list_objects(
            bucket, prefix=prefix, recursive=True
        )
    }

    if manifest_key in existing_keys:
        raise FileExistsError("Completion manifest already exists")

    expected_keys = set()

    with tempfile.TemporaryDirectory(
        prefix="tlcn_coffee_publish_verify_"
    ) as tmp:
        for index, entry in enumerate(parquet_manifest):
            key = entry["object_key"]
            expected_size = entry["size_bytes"]
            expected_sha256 = entry["sha256"]

            if (
                not isinstance(key, str)
                or not key.startswith(prefix)
                or not key.endswith(".parquet")
                or key in expected_keys
            ):
                raise ValueError(f"Invalid Parquet key: {key}")

            expected_keys.add(key)

            if key not in existing_keys:
                raise FileNotFoundError(f"Missing Parquet: {key}")

            local_path = Path(tmp) / f"verify_{index}.parquet"
            client.fget_object(bucket, key, str(local_path))

            if local_path.stat().st_size != expected_size:
                raise ValueError(f"Parquet size mismatch: {key}")

            digest = hashlib.sha256()
            with local_path.open("rb") as stream:
                for chunk in iter(
                    lambda: stream.read(1024 * 1024), b""
                ):
                    digest.update(chunk)

            if digest.hexdigest() != expected_sha256:
                raise ValueError(f"Parquet SHA256 mismatch: {key}")

    actual_parquet_keys = {
        key for key in existing_keys
        if key.endswith(".parquet")
    }

    if actual_parquet_keys != expected_keys:
        raise ValueError("Unexpected or missing Parquet objects")

    manifest = {
        "run_id": run_id,
        "status": "COMPLETED",
        "completed_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "parquet_manifest": parquet_manifest,
        "dq_report": dq_report,
    }

    payload = json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
        default=str,
    ).encode("utf-8")

    client.put_object(
        bucket,
        manifest_key,
        BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    return manifest_key


def run_coffee_pipeline(dry_run=True):
    """Validate the complete Coffee Silver transformation without publishing."""
    if not dry_run:
        raise NotImplementedError(
            "Official Silver publishing is not enabled yet"
        )

    from src.bronze.minio_client import (
        get_minio_client,
        get_bucket_name,
    )

    contract, _ = load_configuration()
    client = get_minio_client()
    bucket = get_bucket_name()
    prefix = contract["source_prefix"]

    raw_records, bronze_metrics = read_bronze_records(
        client, bucket, prefix
    )

    valid_rows = []
    quarantine_rows = []

    for raw in raw_records:
        try:
            normalized = normalize_coffee_record(raw)
            errors = validate_coffee_record(normalized)
        except (ValueError, TypeError, OverflowError) as exc:
            errors = [
                f"normalization_error:{type(exc).__name__}"
            ]

        if errors:
            quarantine_rows.append({
                "bronze_object_key": raw.get(
                    "bronze_object_key"
                ),
                "bronze_row_index": raw.get(
                    "bronze_row_index"
                ),
                "errors": errors,
            })
        else:
            valid_rows.append(normalized)

    silver_rows, dedup_metrics = deduplicate_coffee_records(
        valid_rows
    )

    run_id = compute_coffee_versioned_run_id(
        client, bucket, prefix
    )

    run_status = get_coffee_run_status(
        client, bucket, run_id
    )

    report = build_coffee_run_report(
        bronze_metrics=bronze_metrics,
        valid_count=len(valid_rows),
        quarantine_rows=quarantine_rows,
        dedup_metrics=dedup_metrics,
        parquet_manifest=[],
        run_id=run_id,
    )

    report["execution"] = {
        "mode": "DRY_RUN",
        "run_status": run_status["status"],
        "silver_written": False,
    }

    return report


def write_and_verify_coffee_parquet(spark, silver_rows, output_dir):
    """Write Coffee Silver Parquet locally and verify its contents."""
    from datetime import datetime, timezone
    from pyspark.sql import functions as F

    if not silver_rows:
        raise ValueError("Cannot write an empty Coffee Silver dataset")

    schema = get_coffee_silver_schema()
    processed_at = datetime.now(timezone.utc)

    columns = [field.name for field in schema.fields]

    prepared_rows = []

    for row in silver_rows:
        prepared = dict(row)
        prepared["silver_processed_at"] = processed_at
        prepared_rows.append(
            tuple(prepared.get(column) for column in columns)
        )

    dataframe = spark.createDataFrame(
        prepared_rows,
        schema=schema,
    )

    dataframe.coalesce(1).write.mode("error").parquet(
        str(output_dir)
    )

    restored = spark.read.parquet(str(output_dir))

    expected_types = [
        (field.name, field.dataType.simpleString())
        for field in schema.fields
    ]

    actual_types = [
        (field.name, field.dataType.simpleString())
        for field in restored.schema.fields
    ]

    key_columns = [
        "series_code",
        "quote_date",
        "source_code",
    ]

    row_count = restored.count()
    unique_key_count = restored.select(
        *key_columns
    ).distinct().count()

    invalid_price_count = restored.filter(
        F.col("price").isNull()
        | (F.col("price") <= F.lit(0))
    ).count()

    required_columns = [
        "series_code",
        "quote_date",
        "source_code",
        "price",
        "bronze_object_key",
        "silver_processed_at",
    ]

    missing_required_count = restored.filter(
        " OR ".join(
            f"{column} IS NULL"
            for column in required_columns
        )
    ).count()

    checks = {
        "row_count": row_count == len(silver_rows),
        "unique_business_keys": unique_key_count == row_count,
        "schema_names_and_types": actual_types == expected_types,
        "positive_prices": invalid_price_count == 0,
        "required_fields_present": missing_required_count == 0,
        "parquet_files_exist": bool(
            list(Path(output_dir).glob("*.parquet"))
        ),
    }

    if not all(checks.values()):
        raise RuntimeError(
            f"Coffee Silver Parquet verification failed: {checks}"
        )

    return {
        "row_count": row_count,
        "unique_business_keys": unique_key_count,
        "checks": checks,
    }


def publish_coffee_silver(
    spark,
    client,
    bucket,
    run_id,
    silver_rows,
    bronze_metrics,
    valid_count,
    quarantine_rows,
    dedup_metrics,
):
    """Publish a validated Coffee Silver run to MinIO."""
    import tempfile
    from pathlib import Path

    run_status = get_coffee_run_status(
        client, bucket, run_id
    )

    if run_status["status"] == "COMPLETED":
        return {
            "status": "SKIPPED",
            "reason": "Run already completed",
            "run_id": run_id,
        }

    if run_status["status"] != "NEW":
        raise RuntimeError(
            "Cannot publish an incomplete run: "
            + run_status["status"]
        )

    prefix = f"silver/coffee_prices/runs/{run_id}/"

    with tempfile.TemporaryDirectory(
        prefix="tlcn_coffee_publish_"
    ) as tmp:
        parquet_dir = Path(tmp) / "parquet"

        parquet_result = write_and_verify_coffee_parquet(
            spark,
            silver_rows,
            parquet_dir,
        )

        if parquet_result["row_count"] != dedup_metrics["output_rows"]:
            raise ValueError("Parquet row count mismatch")

        parquet_manifest = upload_verified_parquet(
            client,
            bucket,
            parquet_dir,
            prefix,
        )

        dq_report = build_coffee_run_report(
            bronze_metrics=bronze_metrics,
            valid_count=valid_count,
            quarantine_rows=quarantine_rows,
            dedup_metrics=dedup_metrics,
            parquet_manifest=parquet_manifest,
            run_id=run_id,
        )

        manifest_key = publish_coffee_run_manifest(
            client,
            bucket,
            run_id,
            parquet_manifest,
            dq_report,
        )

    final_status = get_coffee_run_status(
        client, bucket, run_id
    )

    if final_status["status"] != "COMPLETED":
        raise RuntimeError(
            "Run did not reach COMPLETED state"
        )

    return {
        "status": "COMPLETED",
        "run_id": run_id,
        "manifest_key": manifest_key,
        "silver_rows": parquet_result["row_count"],
        "parquet_files": len(parquet_manifest),
    }

def main():
    contract, dq_rules = load_configuration()

    print("PIPELINE: silver_coffee_prices")
    print("CONTRACT_VERSION:", contract["contract_version"])
    print("DQ_VERSION:", dq_rules["version"])
    print("SOURCE_PREFIX:", contract["source_prefix"])
    print("TARGET:", contract["table"])
    print("BUSINESS_KEY:", contract["business_key"])
    print("CONFIG_VALIDATION: PASS")


if __name__ == "__main__":
    main()
