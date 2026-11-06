"""Weather daily Silver ETL for the TLCN Data Lakehouse."""

import json
from pathlib import Path


CONTRACT_PATH = Path(
    "reports/silver/weather_daily_data_contract_v1.json"
)

DQ_PATH = Path(
    "reports/silver/silver_dq_rules_v1.json"
)


def load_configuration():
    """Load and cross-check Weather Silver contract and DQ rules."""
    contract = json.loads(
        CONTRACT_PATH.read_text(encoding="utf-8-sig")
    )
    dq_spec = json.loads(
        DQ_PATH.read_text(encoding="utf-8-sig")
    )

    if contract.get("table") != "silver/weather_daily":
        raise ValueError("Unexpected Weather Silver table")

    if contract.get("format") != "parquet":
        raise ValueError("Weather Silver format must be parquet")

    columns = contract.get("columns")

    if not isinstance(columns, dict) or len(columns) != 16:
        raise ValueError("Weather Silver requires 16 columns")

    expected_key = ["province", "observation_date"]

    if contract.get("business_key") != expected_key:
        raise ValueError("Unexpected Weather Silver business key")

    sources = contract.get("source_prefixes")

    expected_provinces = {
        "dak_lak",
        "dak_nong",
        "gia_lai",
        "lam_dong",
    }

    if not isinstance(sources, dict):
        raise ValueError("Weather source prefixes must be a dictionary")

    if set(sources) != expected_provinces:
        raise ValueError("Unexpected Weather source provinces")

    if any(
        not isinstance(prefix, str)
        or not prefix.startswith("bronze/weather/source=")
        or not prefix.endswith("/")
        for prefix in sources.values()
    ):
        raise ValueError("Invalid Weather Bronze source prefix")

    if contract.get("partition_columns") != ["province"]:
        raise ValueError("Weather Silver must partition by province")

    weather_dq = dq_spec.get("tables", {}).get("weather_daily")

    if not isinstance(weather_dq, dict):
        raise ValueError("Weather DQ configuration is missing")

    if weather_dq.get("business_key") != expected_key:
        raise ValueError("Weather DQ business key mismatch")

    if weather_dq.get("duplicate_policy") != (
        "identical_values_keep_one_conflicting_values_quarantine"
    ):
        raise ValueError("Unexpected Weather duplicate policy")

    allowed_codes = weather_dq.get("allowed_wmo_weather_codes")

    if (
        not isinstance(allowed_codes, list)
        or not allowed_codes
        or not all(isinstance(code, int) for code in allowed_codes)
    ):
        raise ValueError("Invalid allowed WMO weather codes")

    return contract, weather_dq



def read_bronze_weather(client=None, bucket=None):
    """Read allowlisted Open-Meteo Bronze JSON into observation records."""
    from src.bronze.minio_client import (
        get_minio_client,
        get_bucket_name,
    )

    contract, _ = load_configuration()

    if client is None:
        client = get_minio_client()

    if bucket is None:
        bucket = get_bucket_name()

    daily_fields = [
        "weather_code",
        "temperature_2m_mean",
        "temperature_2m_min",
        "temperature_2m_max",
        "rain_sum",
        "precipitation_sum",
        "et0_fao_evapotranspiration",
    ]

    records = []
    source_files = []

    for province, prefix in sorted(
        contract["source_prefixes"].items()
    ):
        objects = sorted(
            (
                obj.object_name
                for obj in client.list_objects(
                    bucket,
                    prefix=prefix,
                    recursive=True,
                )
                if "/raw/" in obj.object_name
                and obj.object_name.endswith(".json")
            )
        )

        for key in objects:
            response = client.get_object(bucket, key)

            try:
                payload = json.loads(
                    response.read().decode("utf-8-sig")
                )
            finally:
                response.close()
                response.release_conn()

            daily = payload.get("daily")

            if not isinstance(daily, dict):
                raise ValueError(
                    f"Missing daily object: {key}"
                )

            dates = daily.get("time")

            if not isinstance(dates, list):
                raise ValueError(
                    f"Missing daily.time array: {key}"
                )

            for field in daily_fields:
                values = daily.get(field)

                if not isinstance(values, list):
                    raise ValueError(
                        f"Missing daily.{field} array: {key}"
                    )

                if len(values) != len(dates):
                    raise ValueError(
                        f"Array length mismatch for {field}: {key}"
                    )

            source_files.append(key)

            for index, observation_date in enumerate(dates):
                row = {
                    "province": province,
                    "observation_date": observation_date,
                    "latitude": payload.get("latitude"),
                    "longitude": payload.get("longitude"),
                    "elevation": payload.get("elevation"),
                    "timezone": payload.get("timezone"),
                    "bronze_object_key": key,
                    "bronze_row_index": index,
                }

                for field in daily_fields:
                    row[field] = daily[field][index]

                records.append(row)

    return {
        "records": records,
        "source_files": source_files,
        "raw_file_count": len(source_files),
        "raw_row_count": len(records),
    }


def normalize_weather_records(records, processed_at=None):
    """Normalize Bronze Weather observations to the 16-column Silver schema."""
    from datetime import date, datetime, timezone

    contract, _ = load_configuration()
    columns = contract["columns"]

    if processed_at is None:
        processed_at = datetime.now(timezone.utc)

    if not isinstance(processed_at, datetime):
        raise TypeError("processed_at must be a datetime")

    if processed_at.tzinfo is None:
        raise ValueError("processed_at must be timezone-aware")

    processed_at = processed_at.astimezone(timezone.utc)

    double_fields = (
        "latitude",
        "longitude",
        "elevation",
        "temperature_2m_mean",
        "temperature_2m_min",
        "temperature_2m_max",
        "rain_sum",
        "precipitation_sum",
        "et0_fao_evapotranspiration",
    )

    normalized = []

    for row_number, row in enumerate(records):
        if not isinstance(row, dict):
            raise TypeError(
                f"Record {row_number} must be a dictionary"
            )

        result = {
            "province": row.get("province"),
            "observation_date": row.get("observation_date"),
            "timezone": row.get("timezone"),
            "weather_code": row.get("weather_code"),
            "bronze_object_key": row.get("bronze_object_key"),
            "bronze_row_index": row.get("bronze_row_index"),
            "silver_processed_at": processed_at,
        }

        value = result["observation_date"]

        if isinstance(value, str):
            try:
                result["observation_date"] = date.fromisoformat(value)
            except ValueError:
                result["observation_date"] = None
        elif isinstance(value, datetime):
            result["observation_date"] = value.date()
        elif value is not None and not isinstance(value, date):
            result["observation_date"] = None

        for field in double_fields:
            value = row.get(field)

            if value is None:
                result[field] = None
            else:
                try:
                    result[field] = float(value)
                except (TypeError, ValueError, OverflowError):
                    result[field] = None

        for field in ("weather_code", "bronze_row_index"):
            value = result[field]

            if value is None:
                continue

            try:
                if isinstance(value, bool):
                    raise ValueError("Boolean is not an integer")

                converted = int(value)

                if str(value).strip() not in (
                    str(converted),
                    f"{converted}.0",
                ):
                    raise ValueError("Non-integral numeric value")

                result[field] = converted
            except (TypeError, ValueError, OverflowError):
                result[field] = None

        if set(result) != set(columns):
            raise ValueError(
                "Normalized fields do not match Weather Silver contract"
            )

        normalized.append({
            name: result[name]
            for name in columns
        })

    return normalized


def validate_and_deduplicate_weather(records):
    """Apply Weather Silver DQ and deterministic deduplication."""
    from collections import defaultdict
    from datetime import date, datetime
    from math import isfinite

    contract, dq = load_configuration()

    required_fields = contract["rules"]["required_fields"]
    allowed_codes = set(dq["allowed_wmo_weather_codes"])
    expected_timezone = dq["checks"]["timezone_equals"]
    comparison_fields = dq["duplicate_comparison_fields"]
    business_key = dq["business_key"]
    expected_provinces = set(contract["source_prefixes"])

    accepted = []
    quarantined = []
    valid_groups = defaultdict(list)

    metrics = {
        "input_rows": len(records),
        "invalid_rows": 0,
        "duplicate_identical_rows": 0,
        "duplicate_conflict_groups": 0,
        "duplicate_conflict_rows": 0,
        "accepted_rows": 0,
        "quarantined_rows": 0,
        "dq_failure_counts": {},
    }

    def quarantine(row, reasons):
        quarantined.append({
            "record": row,
            "reasons": sorted(set(reasons)),
        })

    for row in records:
        reasons = []

        if not isinstance(row, dict):
            quarantine(row, ["invalid_record_type"])
            metrics["invalid_rows"] += 1
            continue

        for field in required_fields:
            if row.get(field) is None:
                reasons.append(f"missing_{field}")

        if row.get("province") not in expected_provinces:
            reasons.append("invalid_province")

        observation_date = row.get("observation_date")
        if type(observation_date) is not date:
            reasons.append("invalid_observation_date")

        if row.get("timezone") != expected_timezone:
            reasons.append("invalid_timezone")

        weather_code = row.get("weather_code")
        if type(weather_code) is not int or weather_code not in allowed_codes:
            reasons.append("invalid_weather_code")

        numeric_fields = [
            "latitude",
            "longitude",
            "elevation",
            "temperature_2m_mean",
            "temperature_2m_min",
            "temperature_2m_max",
            "rain_sum",
            "precipitation_sum",
            "et0_fao_evapotranspiration",
        ]

        for field in numeric_fields:
            value = row.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not isfinite(value)
            ):
                reasons.append(f"invalid_{field}")

        if not any(
            f"invalid_{field}" in reasons
            for field in (
                "temperature_2m_min",
                "temperature_2m_mean",
                "temperature_2m_max",
            )
        ):
            if not (
                row["temperature_2m_min"]
                <= row["temperature_2m_mean"]
                <= row["temperature_2m_max"]
            ):
                reasons.append("invalid_temperature_order")

        for field in (
            "rain_sum",
            "precipitation_sum",
            "et0_fao_evapotranspiration",
        ):
            if f"invalid_{field}" not in reasons:
                if row[field] < 0:
                    reasons.append(f"negative_{field}")

        if not row.get("bronze_object_key"):
            reasons.append("missing_bronze_object_key")

        lineage_index = row.get("bronze_row_index")
        if (
            type(lineage_index) is not int
            or lineage_index < 0
        ):
            reasons.append("invalid_bronze_row_index")

        processed_at = row.get("silver_processed_at")
        if (
            not isinstance(processed_at, datetime)
            or processed_at.tzinfo is None
        ):
            reasons.append("invalid_silver_processed_at")

        if reasons:
            quarantine(row, reasons)
            metrics["invalid_rows"] += 1
            for reason in set(reasons):
                counts = metrics["dq_failure_counts"]
                counts[reason] = counts.get(reason, 0) + 1
            continue

        key = tuple(row[field] for field in business_key)
        valid_groups[key].append(row)

    for key in sorted(valid_groups):
        group = sorted(
            valid_groups[key],
            key=lambda row: (
                row["bronze_object_key"],
                row["bronze_row_index"],
            ),
        )

        reference = tuple(
            group[0].get(field)
            for field in comparison_fields
        )

        identical = all(
            tuple(row.get(field) for field in comparison_fields)
            == reference
            for row in group[1:]
        )

        if identical:
            accepted.append(group[0])
            metrics["duplicate_identical_rows"] += len(group) - 1
        else:
            metrics["duplicate_conflict_groups"] += 1
            metrics["duplicate_conflict_rows"] += len(group)
            for row in group:
                quarantine(row, ["conflicting_business_key"])

    metrics["accepted_rows"] = len(accepted)
    metrics["quarantined_rows"] = len(quarantined)

    if (
        metrics["accepted_rows"]
        + metrics["quarantined_rows"]
        + metrics["duplicate_identical_rows"]
        != metrics["input_rows"]
    ):
        raise ValueError("Weather DQ row accounting mismatch")

    return {
        "accepted": accepted,
        "quarantined": quarantined,
        "metrics": metrics,
    }


def build_weather_spark_schema():
    """Build the 16-column PySpark schema from the Weather contract."""
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        TimestampType,
    )

    contract, _ = load_configuration()

    type_mapping = {
        "string": StringType,
        "date": DateType,
        "double": DoubleType,
        "integer": IntegerType,
        "long": LongType,
        "timestamp_utc": TimestampType,
    }

    required_fields = set(
        contract["rules"]["required_fields"]
    )

    required_fields.update({
        "latitude",
        "longitude",
        "timezone",
        "bronze_object_key",
        "bronze_row_index",
        "silver_processed_at",
    })

    fields = []

    for name, dtype in contract["columns"].items():
        if dtype not in type_mapping:
            raise ValueError(
                f"Unsupported Spark data type for {name}: {dtype}"
            )

        fields.append(
            StructField(
                name,
                type_mapping[dtype](),
                nullable=name not in required_fields,
            )
        )

    schema = StructType(fields)

    if len(schema.fields) != 16:
        raise ValueError(
            "Weather Silver Spark schema must contain 16 columns"
        )

    if schema.fieldNames() != list(contract["columns"]):
        raise ValueError(
            "Spark schema column order does not match contract"
        )

    return schema


def write_weather_parquet_local(
    spark,
    accepted_records,
    output_path,
):
    """Write validated Weather Silver records as province-partitioned Parquet."""
    from pathlib import Path

    contract, _ = load_configuration()
    schema = build_weather_spark_schema()

    if not accepted_records:
        raise ValueError("Cannot write empty Weather Silver dataset")

    destination = Path(output_path).resolve()

    if destination.exists():
        raise FileExistsError(
            f"Weather Parquet destination already exists: {destination}"
        )

    if contract["partition_columns"] != ["province"]:
        raise ValueError("Unexpected Weather partition configuration")

    df = spark.createDataFrame(
        accepted_records,
        schema=schema,
    )

    (
        df.repartition(
            len(contract["source_prefixes"]),
            "province",
        )
        .write
        .mode("errorifexists")
        .partitionBy("province")
        .parquet(str(destination))
    )

    parquet_files = sorted(
        destination.rglob("*.parquet")
    )

    partitions = sorted(
        directory.name
        for directory in destination.iterdir()
        if directory.is_dir()
        and directory.name.startswith("province=")
    )

    expected_partitions = sorted(
        f"province={province}"
        for province in contract["source_prefixes"]
    )

    if partitions != expected_partitions:
        raise RuntimeError(
            f"Unexpected Weather partitions: {partitions}"
        )

    if len(parquet_files) != len(expected_partitions):
        raise RuntimeError(
            "Expected exactly one Parquet file per province"
        )

    return {
        "output_path": str(destination),
        "parquet_files": [str(p) for p in parquet_files],
        "partitions": partitions,
        "file_count": len(parquet_files),
    }


def upload_weather_parquet_to_minio(
    local_parquet_path,
    run_prefix,
    client=None,
    bucket=None,
):
    """Upload province-partitioned Parquet with SHA-256 verification."""
    import hashlib
    from pathlib import Path

    from src.bronze.minio_client import (
        get_minio_client,
        get_bucket_name,
    )

    contract, _ = load_configuration()

    if client is None:
        client = get_minio_client()

    if bucket is None:
        bucket = get_bucket_name()

    root = Path(local_parquet_path).resolve()

    if not root.is_dir():
        raise FileNotFoundError(
            f"Weather Parquet directory not found: {root}"
        )

    expected_provinces = set(contract["source_prefixes"])
    expected_partitions = {
        f"province={province}" for province in expected_provinces
    }

    actual_partitions = {
        directory.name
        for directory in root.iterdir()
        if directory.is_dir()
        and directory.name.startswith("province=")
    }

    if actual_partitions != expected_partitions:
        raise ValueError(
            f"Unexpected Weather partitions: {actual_partitions}"
        )

    parquet_files = sorted(root.rglob("*.parquet"))

    if len(parquet_files) != 4:
        raise ValueError(
            f"Expected 4 Weather Parquet files, got {len(parquet_files)}"
        )

    prefix = run_prefix.strip("/")

    if not prefix.startswith("silver/weather_daily/runs/"):
        raise ValueError("Invalid Weather Silver run prefix")

    if not prefix.endswith("/data"):
        raise ValueError(
            "Weather Parquet run prefix must end with /data"
        )

    planned = []

    for file_path in parquet_files:
        relative = file_path.relative_to(root)

        if len(relative.parts) != 2:
            raise ValueError(
                f"Unexpected Parquet file location: {relative}"
            )

        partition, filename = relative.parts

        if partition not in expected_partitions:
            raise ValueError(
                f"Unexpected Weather partition: {partition}"
            )

        if not filename.endswith(".parquet"):
            raise ValueError("Unexpected Parquet filename")

        key = f"{prefix}/{relative.as_posix()}"

        digest = hashlib.sha256(
            file_path.read_bytes()
        ).hexdigest()

        planned.append({
            "local_path": file_path,
            "object_key": key,
            "sha256": digest,
            "size_bytes": file_path.stat().st_size,
        })

    # Preflight: do not start uploading if any destination exists.
    for item in planned:
        try:
            client.stat_object(bucket, item["object_key"])
        except Exception as exc:
            from minio.error import S3Error

            if not (
                isinstance(exc, S3Error)
                and exc.code in ("NoSuchKey", "NoSuchObject")
            ):
                raise
        else:
            raise FileExistsError(
                f"MinIO object already exists: {item['object_key']}"
            )

    uploaded = []

    for item in planned:
        client.fput_object(
            bucket,
            item["object_key"],
            str(item["local_path"]),
            content_type="application/octet-stream",
        )

        response = client.get_object(
            bucket,
            item["object_key"],
        )

        try:
            remote_hash = hashlib.sha256()

            for chunk in response.stream(1024 * 1024):
                remote_hash.update(chunk)
        finally:
            response.close()
            response.release_conn()

        if remote_hash.hexdigest() != item["sha256"]:
            raise RuntimeError(
                f"SHA-256 mismatch: {item['object_key']}"
            )

        uploaded.append({
            "object_key": item["object_key"],
            "sha256": item["sha256"],
            "size_bytes": item["size_bytes"],
            "verified": True,
        })

    return {
        "bucket": bucket,
        "run_prefix": prefix,
        "file_count": len(uploaded),
        "files": uploaded,
    }


def build_weather_publication_manifest(
    run_id,
    bronze_result,
    dq_result,
    upload_result,
):
    """Build a verified Weather Silver publication manifest."""
    from datetime import datetime, timezone

    contract, _ = load_configuration()

    if not isinstance(run_id, str) or not run_id.startswith("weather_"):
        raise ValueError("Invalid Weather Silver run ID")

    expected_prefix = (
        f"silver/weather_daily/runs/{run_id}/data"
    )

    if upload_result.get("run_prefix") != expected_prefix:
        raise ValueError("Weather upload prefix does not match run ID")

    files = upload_result.get("files", [])

    if len(files) != 4 or upload_result.get("file_count") != 4:
        raise ValueError("Weather publication requires 4 Parquet files")

    expected_partitions = set(contract["source_prefixes"])
    actual_partitions = set()

    for item in files:
        key = item.get("object_key", "")
        relative = key.removeprefix(expected_prefix + "/")
        parts = relative.split("/")

        if (
            not key.startswith(expected_prefix + "/")
            or len(parts) != 2
            or not parts[0].startswith("province=")
            or not parts[1].endswith(".parquet")
        ):
            raise ValueError(f"Invalid Weather Parquet key: {key}")

        province = parts[0].removeprefix("province=")

        if province in actual_partitions:
            raise ValueError(f"Duplicate Weather partition: {province}")

        actual_partitions.add(province)

        sha256 = item.get("sha256", "")

        if (
            item.get("verified") is not True
            or not isinstance(sha256, str)
            or len(sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in sha256)
            or not isinstance(item.get("size_bytes"), int)
            or item["size_bytes"] <= 0
        ):
            raise ValueError(f"Unverified Weather Parquet file: {key}")

    if actual_partitions != expected_partitions:
        raise ValueError("Weather publication partitions mismatch")

    metrics = dq_result["metrics"]

    if metrics["input_rows"] != bronze_result["raw_row_count"]:
        raise ValueError("Weather input row count mismatch")

    if metrics["accepted_rows"] != len(dq_result["accepted"]):
        raise ValueError("Weather accepted row count mismatch")

    if metrics["quarantined_rows"] != len(dq_result["quarantined"]):
        raise ValueError("Weather quarantine row count mismatch")

    if (
        metrics["accepted_rows"]
        + metrics["quarantined_rows"]
        + metrics["duplicate_identical_rows"]
        != metrics["input_rows"]
    ):
        raise ValueError("Weather DQ row accounting mismatch")

    baseline = contract["bronze_baseline"]

    if bronze_result["raw_file_count"] != baseline["raw_files"]:
        raise ValueError("Weather Bronze file count differs from baseline")

    if metrics["input_rows"] != baseline["total_rows"]:
        raise ValueError("Weather Bronze row count differs from baseline")

    if metrics["accepted_rows"] != baseline["distinct_business_keys"]:
        raise ValueError("Weather accepted rows differ from baseline")

    if metrics["quarantined_rows"] != 0:
        raise ValueError("Weather quarantine must be empty for publication")

    if metrics["invalid_rows"] != 0:
        raise ValueError("Weather invalid rows must be zero for publication")

    if metrics["duplicate_conflict_groups"] != 0:
        raise ValueError("Weather duplicate conflicts block publication")

    return {
        "manifest_version": "1.0",
        "status": "COMPLETED",
        "table": contract["table"],
        "run_id": run_id,
        "bucket": upload_result["bucket"],
        "run_prefix": f"silver/weather_daily/runs/{run_id}/",
        "data_prefix": expected_prefix + "/",
        "format": contract["format"],
        "contract_version": contract["contract_version"],
        "contract_file": str(CONTRACT_PATH).replace("\\", "/"),
        "dq_spec_file": str(DQ_PATH).replace("\\", "/"),
        "business_key": contract["business_key"],
        "partition_columns": contract["partition_columns"],
        "schema_columns": len(contract["columns"]),
        "source_prefixes": contract["source_prefixes"],
        "bronze_raw_files": bronze_result["raw_file_count"],
        "bronze_raw_rows": bronze_result["raw_row_count"],
        "dq_metrics": metrics,
        "accepted_rows": metrics["accepted_rows"],
        "quarantined_rows": metrics["quarantined_rows"],
        "parquet_file_count": len(files),
        "parquet_files": files,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def build_weather_run_id(bronze_result):
    """Build a deterministic run ID from Bronze content and configuration."""
    import hashlib
    import json
    from datetime import date, datetime
    from decimal import Decimal

    contract, dq = load_configuration()

    source_files = bronze_result.get("source_files")
    records = bronze_result.get("records")

    if not isinstance(source_files, list) or not source_files:
        raise ValueError("Weather Bronze source file list is empty")

    if len(source_files) != len(set(source_files)):
        raise ValueError("Duplicate Bronze object keys in source list")

    if len(source_files) != bronze_result.get("raw_file_count"):
        raise ValueError("Bronze source file count mismatch")

    if not isinstance(records, list):
        raise ValueError("Weather Bronze records must be a list")

    if bronze_result.get("raw_row_count") != len(records):
        raise ValueError("Bronze source row count mismatch")

    def serialize_value(value):
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(
            f"Unsupported Bronze value type: {type(value).__name__}"
        )

    record_hashes = []

    for record in records:
        serialized_record = json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=serialize_value,
            allow_nan=False,
        ).encode("utf-8")

        record_hashes.append(
            hashlib.sha256(serialized_record).hexdigest()
        )

    content_digest = hashlib.sha256(
        "\n".join(sorted(record_hashes)).encode("ascii")
    ).hexdigest()

    identity = {
        "table": contract["table"],
        "contract": contract,
        "dq": dq,
        "source_files": sorted(source_files),
        "raw_row_count": bronze_result["raw_row_count"],
        "bronze_content_sha256": content_digest,
    }

    serialized = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=serialize_value,
        allow_nan=False,
    ).encode("utf-8")

    digest = hashlib.sha256(serialized).hexdigest()[:24]

    return f"weather_{digest}"



def check_weather_run_destination(
    run_id,
    client=None,
    bucket=None,
):
    """Reject publication if any object already exists under the run prefix."""
    from src.bronze.minio_client import (
        get_minio_client,
        get_bucket_name,
    )

    if (
        not isinstance(run_id, str)
        or not run_id.startswith("weather_")
        or len(run_id) != len("weather_") + 24
        or any(
            ch not in "0123456789abcdef"
            for ch in run_id[len("weather_"):]
        )
    ):
        raise ValueError("Invalid deterministic Weather run ID")

    if client is None:
        client = get_minio_client()

    if bucket is None:
        bucket = get_bucket_name()

    run_prefix = f"silver/weather_daily/runs/{run_id}/"

    existing = next(
        iter(
            client.list_objects(
                bucket,
                prefix=run_prefix,
                recursive=True,
            )
        ),
        None,
    )

    if existing is not None:
        raise FileExistsError(
            f"Weather Silver run already exists: {run_prefix}"
        )

    return {
        "bucket": bucket,
        "run_id": run_id,
        "run_prefix": run_prefix,
        "data_prefix": run_prefix + "data",
        "manifest_key": run_prefix + "manifest.json",
        "destination_available": True,
    }


def publish_weather_manifest(
    manifest,
    client=None,
    bucket=None,
):
    """Publish a verified Weather Silver completion manifest."""
    import hashlib
    import io
    import json

    from minio.error import S3Error
    from src.bronze.minio_client import (
        get_minio_client,
        get_bucket_name,
    )

    if client is None:
        client = get_minio_client()

    if bucket is None:
        bucket = get_bucket_name()

    run_id = manifest.get("run_id")

    if (
        not isinstance(run_id, str)
        or not run_id.startswith("weather_")
        or len(run_id) != 32
        or any(
            ch not in "0123456789abcdef"
            for ch in run_id[8:]
        )
    ):
        raise ValueError("Invalid Weather run ID")

    run_prefix = f"silver/weather_daily/runs/{run_id}/"
    manifest_key = run_prefix + "manifest.json"
    data_prefix = run_prefix + "data/"

    if manifest.get("status") != "COMPLETED":
        raise ValueError("Manifest status must be COMPLETED")

    if manifest.get("table") != "silver/weather_daily":
        raise ValueError("Unexpected Weather Silver table")

    if manifest.get("run_prefix") != run_prefix:
        raise ValueError("Manifest run prefix mismatch")

    if manifest.get("data_prefix") != data_prefix:
        raise ValueError("Manifest data prefix mismatch")

    if manifest.get("bucket") != bucket:
        raise ValueError("Manifest bucket mismatch")

    if manifest.get("accepted_rows") != 39132:
        raise ValueError("Unexpected Weather accepted row count")

    files = manifest.get("parquet_files")

    if not isinstance(files, list) or len(files) != 4:
        raise ValueError("Manifest must contain four Parquet files")

    if manifest.get("parquet_file_count") != len(files):
        raise ValueError("Manifest Parquet file count mismatch")

    # Reject existing manifest before checking uploaded Parquet files.
    try:
        client.stat_object(bucket, manifest_key)
    except S3Error as exc:
        if exc.code not in ("NoSuchKey", "NoSuchObject"):
            raise
    else:
        raise FileExistsError(
            f"Weather manifest already exists: {manifest_key}"
        )

    expected_partitions = {
        "province=dak_lak",
        "province=dak_nong",
        "province=gia_lai",
        "province=lam_dong",
    }

    actual_partitions = set()

    for item in files:
        key = item.get("object_key", "")

        if not key.startswith(data_prefix):
            raise ValueError(f"Invalid Weather object key: {key}")

        relative = key[len(data_prefix):]
        parts = relative.split("/")

        if len(parts) != 2 or not parts[1].endswith(".parquet"):
            raise ValueError(f"Invalid Parquet path: {key}")

        partition = parts[0]

        if partition not in expected_partitions:
            raise ValueError(f"Unexpected partition: {partition}")

        if partition in actual_partitions:
            raise ValueError(f"Duplicate partition: {partition}")

        actual_partitions.add(partition)

        if item.get("verified") is not True:
            raise ValueError(f"Unverified Parquet file: {key}")

        expected_hash = item.get("sha256", "")

        if (
            not isinstance(expected_hash, str)
            or len(expected_hash) != 64
            or any(ch not in "0123456789abcdef" for ch in expected_hash)
        ):
            raise ValueError(f"Invalid SHA-256: {key}")

        metadata = client.stat_object(bucket, key)

        if metadata.size != item.get("size_bytes"):
            raise RuntimeError(f"Parquet size mismatch: {key}")

        response = client.get_object(bucket, key)

        try:
            digest = hashlib.sha256()

            for chunk in response.stream(1024 * 1024):
                digest.update(chunk)
        finally:
            response.close()
            response.release_conn()

        if digest.hexdigest() != expected_hash:
            raise RuntimeError(f"Parquet SHA-256 mismatch: {key}")

    if actual_partitions != expected_partitions:
        raise ValueError("Incomplete Weather partitions")

    payload = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    ).encode("utf-8")

    payload_hash = hashlib.sha256(payload).hexdigest()

    client.put_object(
        bucket,
        manifest_key,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    response = client.get_object(bucket, manifest_key)

    try:
        remote_payload = response.read()
    finally:
        response.close()
        response.release_conn()

    if hashlib.sha256(remote_payload).hexdigest() != payload_hash:
        raise RuntimeError("Weather manifest SHA-256 mismatch")

    return {
        "bucket": bucket,
        "manifest_key": manifest_key,
        "manifest_sha256": payload_hash,
        "manifest_size_bytes": len(payload),
        "verified": True,
        "status": "COMPLETED",
    }


def publish_weather_silver(
    spark,
    client=None,
    bucket=None,
    local_output_root=None,
):
    """Publish validated Weather Silver Parquet and completion manifest."""
    import tempfile
    from pathlib import Path

    from src.bronze.minio_client import (
        get_minio_client,
        get_bucket_name,
    )

    if client is None:
        client = get_minio_client()

    if bucket is None:
        bucket = get_bucket_name()

    bronze_result = read_bronze_weather(
        client=client,
        bucket=bucket,
    )

    normalized = normalize_weather_records(
        bronze_result["records"]
    )

    dq_result = validate_and_deduplicate_weather(
        normalized
    )

    if dq_result["quarantined"]:
        raise RuntimeError(
            "Weather Silver publication blocked: quarantine is not empty"
        )

    run_id = build_weather_run_id(bronze_result)

    destination = check_weather_run_destination(
        run_id,
        client=client,
        bucket=bucket,
    )

    if local_output_root is None:
        local_output_root = Path(tempfile.gettempdir())

    output_path = (
        Path(local_output_root).resolve()
        / f"tlcn-weather-silver-{run_id}"
    )

    if output_path.exists():
        raise FileExistsError(
            f"Local Weather Silver output already exists: {output_path}"
        )

    local_result = write_weather_parquet_local(
        spark,
        dq_result["accepted"],
        output_path,
    )

    upload_result = upload_weather_parquet_to_minio(
        local_parquet_path=output_path,
        run_prefix=destination["data_prefix"],
        client=client,
        bucket=bucket,
    )

    manifest = build_weather_publication_manifest(
        run_id=run_id,
        bronze_result=bronze_result,
        dq_result=dq_result,
        upload_result=upload_result,
    )

    publication = publish_weather_manifest(
        manifest=manifest,
        client=client,
        bucket=bucket,
    )

    return {
        "run_id": run_id,
        "bucket": bucket,
        "run_prefix": destination["run_prefix"],
        "local_output_path": local_result["output_path"],
        "bronze_raw_files": bronze_result["raw_file_count"],
        "bronze_raw_rows": bronze_result["raw_row_count"],
        "accepted_rows": len(dq_result["accepted"]),
        "quarantined_rows": len(dq_result["quarantined"]),
        "parquet_file_count": upload_result["file_count"],
        "manifest_key": publication["manifest_key"],
        "manifest_sha256": publication["manifest_sha256"],
        "status": publication["status"],
    }

def main():
    contract, weather_dq = load_configuration()

    print("TABLE:", contract["table"])
    print("COLUMN_COUNT:", len(contract["columns"]))
    print("BUSINESS_KEY:", contract["business_key"])
    print("PROVINCES:", sorted(contract["source_prefixes"]))
    print("ALLOWED_WMO_CODES:", len(
        weather_dq["allowed_wmo_weather_codes"]
    ))
    print("WEATHER_SILVER_CONFIGURATION: PASS")


if __name__ == "__main__":
    main()
