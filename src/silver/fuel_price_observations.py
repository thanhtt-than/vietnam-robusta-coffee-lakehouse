"""Silver transformation pipeline for Fuel Price Observations."""

import json
from pathlib import Path


CONTRACT_PATH = Path(
    "reports/silver/fuel_price_observations_data_contract_v1.json"
)

DQ_RULES_PATH = Path(
    "reports/silver/silver_dq_rules_v1.json"
)

EXPECTED_TABLE = "silver/fuel_price_observations"

EXPECTED_BUSINESS_KEY = [
    "series_code",
    "quoted_at",
    "source_code",
]


def load_configuration():
    """Load and validate the Fuel Silver contract and DQ rules."""
    contract = json.loads(
        CONTRACT_PATH.read_text(encoding="utf-8-sig")
    )

    dq_rules = json.loads(
        DQ_RULES_PATH.read_text(encoding="utf-8-sig")
    )

    fuel_rules = dq_rules["tables"]["fuel_price_observations"]

    if contract["table"] != EXPECTED_TABLE:
        raise ValueError("Unexpected Fuel Silver table")

    if contract["format"] != "parquet":
        raise ValueError("Fuel Silver format must be parquet")

    if contract["business_key"] != EXPECTED_BUSINESS_KEY:
        raise ValueError("Unexpected Fuel business key")

    if fuel_rules["business_key"] != EXPECTED_BUSINESS_KEY:
        raise ValueError("Fuel DQ business key mismatch")

    if len(contract["columns"]) != 18:
        raise ValueError("Fuel Silver schema must contain 18 columns")

    if contract["source_prefix"] != (
        "bronze/fuel_prices/source=bbw_fuel/"
    ):
        raise ValueError("Unexpected Fuel Bronze source prefix")

    return contract, dq_rules



def read_bronze_records(client, bucket, prefix):
    """Read Fuel Bronze JSON records with deterministic lineage."""
    import json

    object_keys = sorted(
        obj.object_name
        for obj in client.list_objects(
            bucket,
            prefix=prefix,
            recursive=True,
        )
        if obj.object_name.endswith(".json")
        and "/raw/" in obj.object_name
        and not obj.object_name.endswith("metadata.json")
    )

    records = []
    empty_files = 0

    for object_key in object_keys:
        response = client.get_object(bucket, object_key)

        try:
            payload = json.loads(
                response.read().decode("utf-8-sig")
            )
        finally:
            response.close()
            response.release_conn()

        if not isinstance(payload, dict):
            raise TypeError(
                f"Bronze payload must be an object: {object_key}"
            )

        data = payload.get("data")

        if not isinstance(data, list):
            raise TypeError(
                f"Bronze data must be an array: {object_key}"
            )

        if not data:
            empty_files += 1

        for row_index, row in enumerate(data):
            if not isinstance(row, dict):
                raise TypeError(
                    f"Invalid Bronze record: {object_key}"
                    f" at index {row_index}"
                )

            record = dict(row)

            record["bronze_object_key"] = object_key
            record["bronze_row_index"] = row_index

            records.append(record)

    metrics = {
        "raw_files": len(object_keys),
        "empty_files": empty_files,
        "total_rows": len(records),
    }

    return records, metrics


def normalize_fuel_record(record):
    """Normalize one Fuel Bronze record into Silver-compatible values."""
    from datetime import date, datetime, timezone
    from decimal import Decimal, InvalidOperation

    if not isinstance(record, dict):
        raise TypeError("Fuel Bronze record must be a dictionary")

    meta = record.get("meta")
    source_info = record.get("source")

    if not isinstance(meta, dict):
        raise TypeError("Fuel meta must be a dictionary")

    if not isinstance(source_info, dict):
        raise TypeError("Fuel source must be a dictionary")

    def parse_utc(value, field_name):
        if not isinstance(value, str):
            raise TypeError(f"{field_name} must be an ISO timestamp string")

        parsed = datetime.fromisoformat(value)

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{field_name} must include timezone")

        return parsed.astimezone(timezone.utc)

    quote_date_raw = record.get("quote_date_vn")

    if not isinstance(quote_date_raw, str):
        raise TypeError("quote_date_vn must be a string")

    quote_date = date.fromisoformat(quote_date_raw)

    try:
        price = Decimal(str(record.get("price")))
        if not price.is_finite():
            raise ValueError("Fuel price must be finite")
        price = price.quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("Invalid Fuel price") from exc

    bronze_row_index = record.get("bronze_row_index")

    if (
        not isinstance(bronze_row_index, int)
        or isinstance(bronze_row_index, bool)
        or bronze_row_index < 0
    ):
        raise ValueError("Invalid bronze_row_index")

    return {
        "product": record.get("product"),
        "region": record.get("region"),
        "series_code": record.get("series_code"),
        "quote_date": quote_date,
        "quoted_at": parse_utc(
            record.get("quoted_at"), "quoted_at"
        ),
        "price": price,
        "currency": record.get("currency"),
        "unit": record.get("unit"),
        "provider": meta.get("provider"),
        "fuel_name": meta.get("fuel_name"),
        "zone": meta.get("zone"),
        "source_page": meta.get("source_page"),
        "source_code": source_info.get("code"),
        "source_name": source_info.get("name"),
        "updated_at": parse_utc(
            record.get("updated_at"), "updated_at"
        ),
        "bronze_object_key": record.get("bronze_object_key"),
        "bronze_row_index": bronze_row_index,
    }


def validate_fuel_record(row):
    """Return DQ violation codes for a normalized Fuel Silver record."""
    from datetime import date, datetime, timedelta
    from decimal import Decimal

    if not isinstance(row, dict):
        return ["invalid_record_type"]

    errors = []

    required_fields = [
        "series_code",
        "quote_date",
        "quoted_at",
        "price",
        "currency",
        "unit",
        "source_code",
        "source_page",
    ]

    for field in required_fields:
        value = row.get(field)

        if value is None or (
            isinstance(value, str) and not value.strip()
        ):
            errors.append(f"missing_{field}")

    expected_values = {
        "product": "FUEL",
        "currency": "VND",
        "unit": "VND/L",
    }

    for field, expected in expected_values.items():
        if row.get(field) != expected:
            errors.append(f"unexpected_{field}")

    quote_date = row.get("quote_date")

    if (
        not isinstance(quote_date, date)
        or isinstance(quote_date, datetime)
    ):
        errors.append("invalid_quote_date")

    for field in ("quoted_at", "updated_at"):
        value = row.get(field)

        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() != timedelta(0)
        ):
            errors.append(f"invalid_{field}")

    price = row.get("price")

    if not isinstance(price, Decimal):
        errors.append("invalid_price_type")
    elif not price.is_finite() or price <= 0:
        errors.append("invalid_price")
    elif (
        price.as_tuple().exponent != -2
        or price >= Decimal("10000000000000000.00")
    ):
        errors.append("invalid_decimal_18_2")

    object_key = row.get("bronze_object_key")

    if (
        not isinstance(object_key, str)
        or not object_key.startswith(
            "bronze/fuel_prices/source=bbw_fuel/"
        )
        or "/raw/" not in object_key
        or not object_key.endswith(".json")
    ):
        errors.append("invalid_bronze_object_key")

    row_index = row.get("bronze_row_index")

    if (
        not isinstance(row_index, int)
        or isinstance(row_index, bool)
        or row_index < 0
    ):
        errors.append("invalid_bronze_row_index")

    return errors


def deduplicate_fuel_records(rows):
    """Deduplicate Fuel records without collapsing distinct timestamps."""
    from collections import defaultdict

    groups = defaultdict(list)

    for row in rows:
        key = (
            row["series_code"],
            row["quoted_at"],
            row["source_code"],
        )
        groups[key].append(row)

    winners = []
    quarantine = []
    duplicate_groups = 0
    conflict_groups = 0
    removed_duplicates = 0

    for key in sorted(groups):
        group = groups[key]

        if len(group) == 1:
            winners.append(group[0])
            continue

        duplicate_groups += 1

        distinct_prices = {
            row["price"] for row in group
        }

        if len(distinct_prices) > 1:
            conflict_groups += 1

            for row in group:
                quarantine.append({
                    "bronze_object_key": row["bronze_object_key"],
                    "bronze_row_index": row["bronze_row_index"],
                    "business_key": {
                        "series_code": key[0],
                        "quoted_at": key[1].isoformat(),
                        "source_code": key[2],
                    },
                    "errors": ["conflicting_price_same_business_key"],
                    "price": str(row["price"]),
                })

            continue

        winner = sorted(
            group,
            key=lambda row: (
                -row["updated_at"].timestamp(),
                row["bronze_object_key"],
                row["bronze_row_index"],
            ),
        )[0]

        winners.append(winner)
        removed_duplicates += len(group) - 1

    metrics = {
        "input_rows": len(rows),
        "output_rows": len(winners),
        "duplicate_key_groups": duplicate_groups,
        "price_conflict_groups": conflict_groups,
        "removed_duplicates": removed_duplicates,
        "conflict_quarantine_rows": len(quarantine),
    }

    if (
        metrics["output_rows"]
        + metrics["removed_duplicates"]
        + metrics["conflict_quarantine_rows"]
        != metrics["input_rows"]
    ):
        raise RuntimeError(
            "Fuel deduplication accounting mismatch"
        )

    return winners, quarantine, metrics


def get_fuel_silver_schema():
    """Return the explicit 18-column Fuel Silver Spark schema."""
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DateType,
        TimestampType,
        DecimalType,
        LongType,
    )

    return StructType([
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("series_code", StringType(), False),
        StructField("quote_date", DateType(), False),
        StructField("quoted_at", TimestampType(), False),
        StructField("price", DecimalType(18, 2), False),
        StructField("currency", StringType(), False),
        StructField("unit", StringType(), False),
        StructField("provider", StringType(), True),
        StructField("fuel_name", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("source_page", StringType(), False),
        StructField("source_code", StringType(), False),
        StructField("source_name", StringType(), True),
        StructField("updated_at", TimestampType(), False),
        StructField("bronze_object_key", StringType(), False),
        StructField("bronze_row_index", LongType(), False),
        StructField("silver_processed_at", TimestampType(), False),
    ])


def write_and_verify_fuel_parquet(
    spark,
    silver_rows,
    output_dir,
    processed_at,
):
    """Write Fuel Silver Parquet locally and verify its contents."""
    from datetime import datetime, timezone
    from pathlib import Path

    from pyspark.sql import functions as F

    schema = get_fuel_silver_schema()
    columns = [field.name for field in schema.fields]

    if (
        not isinstance(processed_at, datetime)
        or processed_at.tzinfo is None
        or processed_at.utcoffset().total_seconds() != 0
    ):
        raise ValueError("processed_at must be a UTC-aware datetime")

    if not silver_rows:
        raise ValueError("Cannot write empty Fuel Silver dataset")

    output_path = Path(output_dir)

    if output_path.exists():
        raise FileExistsError(
            f"Refusing to overwrite existing path: {output_path}"
        )

    prepared = []

    for row in silver_rows:
        item = dict(row)
        item["silver_processed_at"] = processed_at
        prepared.append(
            tuple(item.get(column) for column in columns)
        )

    dataframe = spark.createDataFrame(
        prepared,
        schema=schema,
    )

    dataframe.coalesce(1).write.mode("error").parquet(
        str(output_path)
    )

    restored = spark.read.parquet(str(output_path))

    row_count = restored.count()

    unique_keys = restored.select(
        "series_code",
        "quoted_at",
        "source_code",
    ).distinct().count()

    invalid_prices = restored.filter(
        F.col("price").isNull()
        | (F.col("price") <= 0)
    ).count()

    missing_lineage = restored.filter(
        F.col("bronze_object_key").isNull()
        | (F.length(F.trim("bronze_object_key")) == 0)
        | F.col("bronze_row_index").isNull()
        | (F.col("bronze_row_index") < 0)
    ).count()

    missing_source_page = restored.filter(
        F.col("source_page").isNull()
        | (F.length(F.trim("source_page")) == 0)
    ).count()

    actual_types = [
        (field.name, field.dataType.simpleString())
        for field in restored.schema.fields
    ]

    expected_types = [
        (field.name, field.dataType.simpleString())
        for field in schema.fields
    ]

    parquet_files = sorted(output_path.glob("*.parquet"))

    checks = {
        "row_count": row_count == len(silver_rows),
        "unique_business_keys": unique_keys == row_count,
        "schema_names_and_types": actual_types == expected_types,
        "positive_prices": invalid_prices == 0,
        "bronze_lineage": missing_lineage == 0,
        "source_page_present": missing_source_page == 0,
        "single_parquet_file": len(parquet_files) == 1,
    }

    if not all(checks.values()):
        raise RuntimeError(
            f"Fuel Parquet verification failed: {checks}"
        )

    parquet_file = parquet_files[0]

    return {
        "parquet_path": str(parquet_file),
        "parquet_size_bytes": parquet_file.stat().st_size,
        "row_count": row_count,
        "unique_business_keys": unique_keys,
        "column_count": len(restored.columns),
        "checks": checks,
    }


def upload_and_verify_fuel_parquet(
    client,
    bucket,
    local_parquet_path,
    object_key,
):
    """Upload Fuel Parquet to MinIO and verify SHA-256 by reading it back."""
    import hashlib
    from pathlib import Path

    from minio.error import S3Error

    local_path = Path(local_parquet_path)

    if not local_path.is_file():
        raise FileNotFoundError(local_path)

    if local_path.stat().st_size <= 0:
        raise ValueError("Cannot upload an empty Parquet file")

    if (
        not isinstance(object_key, str)
        or not object_key.startswith("silver/fuel_price_observations/runs/")
        or not object_key.endswith(".parquet")
    ):
        raise ValueError("Invalid Fuel Silver object key")

    local_hasher = hashlib.sha256()

    with local_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            local_hasher.update(chunk)

    local_sha256 = local_hasher.hexdigest()
    local_size = local_path.stat().st_size

    try:
        client.stat_object(bucket, object_key)
    except S3Error as exc:
        if exc.code not in ("NoSuchKey", "NoSuchObject", "NoSuchVersion"):
            raise
    else:
        raise FileExistsError(
            f"Refusing to overwrite MinIO object: {object_key}"
        )

    with local_path.open("rb") as stream:
        client.put_object(
            bucket,
            object_key,
            stream,
            length=local_size,
            content_type="application/octet-stream",
            metadata={"sha256": local_sha256},
        )

    response = client.get_object(bucket, object_key)

    remote_hasher = hashlib.sha256()
    remote_size = 0

    try:
        for chunk in response.stream(1024 * 1024):
            remote_hasher.update(chunk)
            remote_size += len(chunk)
    finally:
        response.close()
        response.release_conn()

    remote_sha256 = remote_hasher.hexdigest()

    checks = {
        "sha256_matches": remote_sha256 == local_sha256,
        "size_matches": remote_size == local_size,
        "nonempty": remote_size > 0,
    }

    if not all(checks.values()):
        raise RuntimeError(
            f"Fuel Parquet MinIO verification failed: {checks}"
        )

    return {
        "object_key": object_key,
        "size_bytes": local_size,
        "sha256": local_sha256,
        "checks": checks,
    }


def build_fuel_silver_manifest(
    run_id,
    processed_at,
    bronze_metrics,
    dedup_metrics,
    parquet_result,
    upload_result,
    quarantine_count,
):
    """Build a verified Fuel Silver run manifest."""
    from datetime import datetime, timezone

    if (
        not isinstance(run_id, str)
        or not run_id.strip()
        or "/" in run_id
        or "\\\\" in run_id
    ):
        raise ValueError("Invalid Fuel Silver run_id")

    if (
        not isinstance(processed_at, datetime)
        or processed_at.tzinfo is None
        or processed_at.utcoffset().total_seconds() != 0
    ):
        raise ValueError("processed_at must be UTC-aware")

    if quarantine_count < 0:
        raise ValueError("Invalid quarantine count")

    required_bronze = (
        "raw_files",
        "empty_files",
        "total_rows",
    )

    required_dedup = (
        "input_rows",
        "output_rows",
        "duplicate_key_groups",
        "price_conflict_groups",
        "removed_duplicates",
        "conflict_quarantine_rows",
    )

    for field in required_bronze:
        if field not in bronze_metrics:
            raise ValueError(f"Missing Bronze metric: {field}")

    for field in required_dedup:
        if field not in dedup_metrics:
            raise ValueError(f"Missing dedup metric: {field}")

    parquet_checks = parquet_result.get("checks", {})
    upload_checks = upload_result.get("checks", {})

    if not parquet_checks or not all(parquet_checks.values()):
        raise ValueError("Parquet verification is incomplete")

    if not upload_checks or not all(upload_checks.values()):
        raise ValueError("MinIO upload verification is incomplete")

    silver_count = dedup_metrics["output_rows"]
    bronze_count = bronze_metrics["total_rows"]

    if (
        bronze_count != dedup_metrics["input_rows"]
        or silver_count != parquet_result["row_count"]
        or silver_count != parquet_result["unique_business_keys"]
        or parquet_result["column_count"] != 18
        or quarantine_count != dedup_metrics["conflict_quarantine_rows"]
        or (
            silver_count
            + dedup_metrics["removed_duplicates"]
            + quarantine_count
        ) != bronze_count
    ):
        raise ValueError("Fuel Silver manifest count mismatch")

    expected_prefix = (
        f"silver/fuel_price_observations/runs/{run_id}/"
    )

    object_key = upload_result["object_key"]

    if (
        not object_key.startswith(expected_prefix)
        or not object_key.endswith(".parquet")
    ):
        raise ValueError("Fuel Parquet object key does not match run_id")

    if (
        upload_result["size_bytes"]
        != parquet_result["parquet_size_bytes"]
    ):
        raise ValueError("Fuel Parquet size mismatch")

    sha256 = upload_result["sha256"]

    if (
        not isinstance(sha256, str)
        or len(sha256) != 64
        or any(char not in "0123456789abcdef" for char in sha256)
    ):
        raise ValueError("Invalid Parquet SHA-256")

    return {
        "table": "silver/fuel_price_observations",
        "format": "parquet",
        "run_id": run_id,
        "status": "COMPLETED",
        "processed_at_utc": processed_at.isoformat(),
        "source_prefix": "bronze/fuel_prices/source=bbw_fuel/",
        "bronze_metrics": dict(bronze_metrics),
        "dedup_metrics": dict(dedup_metrics),
        "silver_row_count": silver_count,
        "quarantine_row_count": quarantine_count,
        "column_count": 18,
        "unique_business_keys": parquet_result["unique_business_keys"],
        "parquet": {
            "object_key": object_key,
            "size_bytes": upload_result["size_bytes"],
            "sha256": sha256,
        },
        "verification": {
            "parquet": dict(parquet_checks),
            "minio_upload": dict(upload_checks),
        },
    }


def publish_fuel_silver_manifest(client, bucket, manifest):
    """Publish a verified Fuel Silver manifest without overwriting."""
    import hashlib
    import io
    import json

    from minio.error import S3Error

    if not isinstance(manifest, dict):
        raise ValueError("Manifest must be a dictionary")

    if (
        manifest.get("table") != "silver/fuel_price_observations"
        or manifest.get("status") != "COMPLETED"
    ):
        raise ValueError("Fuel Silver manifest is not COMPLETED")

    run_id = manifest.get("run_id")

    if (
        not isinstance(run_id, str)
        or not run_id
        or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in run_id)
    ):
        raise ValueError("Invalid Fuel Silver run_id")

    prefix = f"silver/fuel_price_observations/runs/{run_id}/"
    manifest_key = prefix + "manifest.json"

    parquet = manifest.get("parquet", {})
    parquet_key = parquet.get("object_key")

    if (
        not isinstance(parquet_key, str)
        or not parquet_key.startswith(prefix)
        or not parquet_key.endswith(".parquet")
    ):
        raise ValueError("Invalid Fuel Silver Parquet object key")

    expected_sha256 = parquet.get("sha256")
    expected_size = parquet.get("size_bytes")

    if (
        not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(c not in "0123456789abcdef" for c in expected_sha256)
        or not isinstance(expected_size, int)
        or isinstance(expected_size, bool)
        or expected_size <= 0
    ):
        raise ValueError("Invalid Fuel Silver Parquet integrity metadata")

    verification = manifest.get("verification", {})

    for section in ("parquet", "minio_upload"):
        checks = verification.get(section)

        if (
            not isinstance(checks, dict)
            or not checks
            or not all(value is True for value in checks.values())
        ):
            raise ValueError(f"Invalid verification section: {section}")

    try:
        client.stat_object(bucket, manifest_key)
    except S3Error as exc:
        if exc.code not in ("NoSuchKey", "NoSuchObject", "NoSuchVersion"):
            raise
    else:
        raise FileExistsError(
            f"Refusing to overwrite manifest: {manifest_key}"
        )

    # Verify the actual remote Parquet before publishing COMPLETED.
    parquet_response = client.get_object(bucket, parquet_key)
    parquet_hasher = hashlib.sha256()
    parquet_size = 0

    try:
        for chunk in parquet_response.stream(1024 * 1024):
            parquet_hasher.update(chunk)
            parquet_size += len(chunk)
    finally:
        parquet_response.close()
        parquet_response.release_conn()

    if (
        parquet_hasher.hexdigest() != expected_sha256
        or parquet_size != expected_size
    ):
        raise RuntimeError(
            "Remote Fuel Parquet integrity verification failed"
        )

    manifest_bytes = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    ).encode("utf-8")

    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

    client.put_object(
        bucket,
        manifest_key,
        io.BytesIO(manifest_bytes),
        length=len(manifest_bytes),
        content_type="application/json",
        metadata={"sha256": manifest_sha256},
    )

    response = client.get_object(bucket, manifest_key)
    remote_hasher = hashlib.sha256()
    remote_size = 0

    try:
        for chunk in response.stream(1024 * 1024):
            remote_hasher.update(chunk)
            remote_size += len(chunk)
    finally:
        response.close()
        response.release_conn()

    checks = {
        "manifest_sha256_matches": (
            remote_hasher.hexdigest() == manifest_sha256
        ),
        "manifest_size_matches": (
            remote_size == len(manifest_bytes)
        ),
        "parquet_sha256_matches": True,
        "parquet_size_matches": True,
    }

    if not all(checks.values()):
        raise RuntimeError(
            f"Fuel manifest publication verification failed: {checks}"
        )

    return {
        "manifest_object_key": manifest_key,
        "manifest_sha256": manifest_sha256,
        "manifest_size_bytes": len(manifest_bytes),
        "checks": checks,
    }


def compute_fuel_silver_run_id(client, bucket, source_prefix):
    """Compute a deterministic run ID from Bronze object keys and bytes."""
    import hashlib

    objects = sorted(
        (
            item for item in client.list_objects(
                bucket,
                prefix=source_prefix,
                recursive=True,
            )
            if item.object_name.endswith(".json")
            and "/raw/" in item.object_name
        ),
        key=lambda item: item.object_name,
    )

    if not objects:
        raise ValueError("No Fuel Bronze JSON objects found")

    hasher = hashlib.sha256()
    hasher.update(b"fuel_silver_v1\n")

    for item in objects:
        key_bytes = item.object_name.encode("utf-8")
        response = client.get_object(bucket, item.object_name)

        object_hasher = hashlib.sha256()

        try:
            for chunk in response.stream(1024 * 1024):
                object_hasher.update(chunk)
        finally:
            response.close()
            response.release_conn()

        hasher.update(len(key_bytes).to_bytes(8, "big"))
        hasher.update(key_bytes)
        hasher.update(object_hasher.digest())

    return "fuel_" + hasher.hexdigest()[:24]


def inspect_fuel_silver_run(client, bucket, run_id):
    """Inspect whether a Fuel Silver run is absent, incomplete, or completed."""
    import json

    if (
        not isinstance(run_id, str)
        or not run_id.startswith("fuel_")
        or len(run_id) != 29
        or any(char not in "0123456789abcdef" for char in run_id[5:])
    ):
        raise ValueError("Invalid deterministic Fuel Silver run ID")

    prefix = f"silver/fuel_price_observations/runs/{run_id}/"

    keys = sorted(
        item.object_name
        for item in client.list_objects(
            bucket,
            prefix=prefix,
            recursive=True,
        )
    )

    manifest_key = prefix + "manifest.json"

    if not keys:
        return {
            "run_id": run_id,
            "status": "NOT_STARTED",
            "object_keys": [],
        }

    if manifest_key not in keys:
        return {
            "run_id": run_id,
            "status": "INCOMPLETE",
            "object_keys": keys,
        }

    response = client.get_object(bucket, manifest_key)

    try:
        manifest = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()

    parquet = manifest.get("parquet", {})
    parquet_key = parquet.get("object_key")

    valid_manifest = (
        manifest.get("table") == "silver/fuel_price_observations"
        and manifest.get("run_id") == run_id
        and manifest.get("status") == "COMPLETED"
        and isinstance(parquet_key, str)
        and parquet_key.startswith(prefix)
        and parquet_key.endswith(".parquet")
        and parquet_key in keys
    )

    return {
        "run_id": run_id,
        "status": "COMPLETED" if valid_manifest else "INCOMPLETE",
        "object_keys": keys,
        "manifest_key": manifest_key,
    }


def dry_run_fuel_silver(client, bucket):
    """Read-only Fuel Silver pre-publication validation."""
    from collections import Counter

    contract, _ = load_configuration()
    source_prefix = contract["source_prefix"]

    records, bronze_metrics = read_bronze_records(
        client, bucket, source_prefix
    )

    normalized = []
    quarantine = []
    normalization_errors = Counter()
    dq_errors = Counter()

    for record in records:
        try:
            row = normalize_fuel_record(record)
        except (ValueError, TypeError, OverflowError, KeyError) as exc:
            code = "normalization_" + type(exc).__name__
            normalization_errors[code] += 1
            quarantine.append({
                "bronze_object_key": record.get("bronze_object_key"),
                "bronze_row_index": record.get("bronze_row_index"),
                "errors": [code],
            })
            continue

        errors = validate_fuel_record(row)

        if errors:
            dq_errors.update(errors)
            quarantine.append({
                "bronze_object_key": row.get("bronze_object_key"),
                "bronze_row_index": row.get("bronze_row_index"),
                "errors": errors,
            })
        else:
            normalized.append(row)

    silver_rows, conflict_quarantine, dedup_metrics = (
        deduplicate_fuel_records(normalized)
    )

    quarantine.extend(conflict_quarantine)

    run_id = compute_fuel_silver_run_id(
        client, bucket, source_prefix
    )

    run_state = inspect_fuel_silver_run(
        client, bucket, run_id
    )

    unique_keys = len({
        (
            row["series_code"],
            row["quoted_at"],
            row["source_code"],
        )
        for row in silver_rows
    })

    checks = {
        "bronze_objects_present": bronze_metrics["raw_files"] > 0,
        "bronze_rows_present": bronze_metrics["total_rows"] > 0,
        "all_rows_accounted_for": (
            len(silver_rows)
            + dedup_metrics["removed_duplicates"]
            + len(quarantine)
            == bronze_metrics["total_rows"]
        ),
        "no_normalization_errors": not normalization_errors,
        "no_dq_errors": not dq_errors,
        "no_conflicting_business_keys": (
            dedup_metrics["price_conflict_groups"] == 0
        ),
        "no_quarantine": not quarantine,
        "unique_business_keys": unique_keys == len(silver_rows),
        "run_not_started": run_state["status"] == "NOT_STARTED",
    }

    return {
        "table": "silver/fuel_price_observations",
        "mode": "DRY_RUN",
        "run_id": run_id,
        "run_status": run_state["status"],
        "bronze_metrics": bronze_metrics,
        "normalized_valid_rows": len(normalized),
        "silver_row_count": len(silver_rows),
        "unique_business_keys": unique_keys,
        "quarantine_row_count": len(quarantine),
        "normalization_errors": dict(normalization_errors),
        "dq_errors": dict(dq_errors),
        "dedup_metrics": dedup_metrics,
        "checks": checks,
        "ready_to_publish": all(checks.values()),
    }


def publish_fuel_silver_run(client, bucket):
    """Publish a validated, versioned Fuel Silver Parquet run."""
    import tempfile
    from datetime import datetime, timezone
    from pathlib import Path

    from pyspark.sql import SparkSession

    dry_run = dry_run_fuel_silver(client, bucket)

    if not dry_run["ready_to_publish"]:
        raise RuntimeError(
            f"Fuel Silver is not ready to publish: {dry_run['checks']}"
        )

    run_id = dry_run["run_id"]
    prefix = f"silver/fuel_price_observations/runs/{run_id}/"

    state = inspect_fuel_silver_run(client, bucket, run_id)

    if state["status"] != "NOT_STARTED":
        raise RuntimeError(
            f"Refusing to publish Fuel run in state: {state['status']}"
        )

    contract, _ = load_configuration()

    records, bronze_metrics = read_bronze_records(
        client,
        bucket,
        contract["source_prefix"],
    )

    normalized = []

    for record in records:
        row = normalize_fuel_record(record)
        errors = validate_fuel_record(row)

        if errors:
            raise RuntimeError(
                f"Unexpected Fuel DQ errors during publication: {errors}"
            )

        normalized.append(row)

    silver_rows, quarantine, dedup_metrics = (
        deduplicate_fuel_records(normalized)
    )

    if quarantine or len(silver_rows) != dry_run["silver_row_count"]:
        raise RuntimeError(
            "Fuel Silver changed between dry-run and publication"
        )

    current_run_id = compute_fuel_silver_run_id(
        client,
        bucket,
        contract["source_prefix"],
    )

    if current_run_id != run_id:
        raise RuntimeError(
            "Fuel Bronze changed during publication preparation"
        )

    processed_at = datetime.now(timezone.utc)

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TLCN-Fuel-Silver-Publish")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        with tempfile.TemporaryDirectory(
            prefix="tlcn_fuel_publish_"
        ) as tmp:
            output_dir = Path(tmp) / "fuel_silver"

            parquet_result = write_and_verify_fuel_parquet(
                spark=spark,
                silver_rows=silver_rows,
                output_dir=output_dir,
                processed_at=processed_at,
            )

            # Check again before the first remote Silver write.
            current_run_id = compute_fuel_silver_run_id(
                client,
                bucket,
                contract["source_prefix"],
            )

            if current_run_id != run_id:
                raise RuntimeError(
                    "Fuel Bronze changed before Parquet upload"
                )

            state = inspect_fuel_silver_run(
                client,
                bucket,
                run_id,
            )

            if state["status"] != "NOT_STARTED":
                raise RuntimeError(
                    "Fuel Silver run is no longer NOT_STARTED"
                )

            parquet_key = prefix + "part-00000.parquet"

            upload_result = upload_and_verify_fuel_parquet(
                client=client,
                bucket=bucket,
                local_parquet_path=parquet_result["parquet_path"],
                object_key=parquet_key,
            )

            manifest = build_fuel_silver_manifest(
                run_id=run_id,
                processed_at=processed_at,
                bronze_metrics=bronze_metrics,
                dedup_metrics=dedup_metrics,
                parquet_result=parquet_result,
                upload_result=upload_result,
                quarantine_count=len(quarantine),
            )

            publication = publish_fuel_silver_manifest(
                client,
                bucket,
                manifest,
            )

            final_state = inspect_fuel_silver_run(
                client,
                bucket,
                run_id,
            )

            if final_state["status"] != "COMPLETED":
                raise RuntimeError(
                    "Fuel Silver run did not reach COMPLETED state"
                )

            return {
                "run_id": run_id,
                "status": final_state["status"],
                "silver_row_count": len(silver_rows),
                "quarantine_row_count": len(quarantine),
                "parquet_object_key": parquet_key,
                "parquet_sha256": upload_result["sha256"],
                "manifest_object_key": publication["manifest_object_key"],
                "manifest_sha256": publication["manifest_sha256"],
                "parquet_checks": parquet_result["checks"],
                "upload_checks": upload_result["checks"],
                "manifest_checks": publication["checks"],
            }

    finally:
        spark.stop()

def main():
    contract, dq_rules = load_configuration()

    print("FUEL_SILVER_TABLE:", contract["table"])
    print("FUEL_SILVER_COLUMNS:", len(contract["columns"]))
    print("FUEL_BUSINESS_KEY:", contract["business_key"])
    print(
        "FUEL_DQ_RULES_PRESENT:",
        "fuel_price_observations" in dq_rules["tables"],
    )
    print("FUEL_CONFIGURATION_TEST: PASS")


if __name__ == "__main__":
    main()
