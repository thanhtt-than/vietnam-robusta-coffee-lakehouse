"""Helpers dùng chung để tạo metadata và ghi dữ liệu gốc vào MinIO Bronze."""
from __future__ import annotations

import hashlib
import json
import mimetypes
import re
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

from src.bronze.minio_client import get_bucket_name, get_minio_client


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def timestamp_id() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]


def safe_segment(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9._=-]+", "_", value)
    return value.strip("._-") or "unknown"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def guess_content_type(filename: str, fallback: str = "application/octet-stream") -> str:
    content_type, _ = mimetypes.guess_type(filename)
    return content_type or fallback


def make_object_prefix(domain: str, source: str, batch_id: str | None = None,
                       ingestion_time: datetime | None = None) -> tuple[str, str]:
    ingestion_time = ingestion_time or utc_now()
    batch_id = batch_id or timestamp_id()
    prefix = (
        f"bronze/{safe_segment(domain)}/source={safe_segment(source)}/"
        f"ingestion_date={ingestion_time:%Y-%m-%d}/batch_id={safe_segment(batch_id)}"
    )
    return prefix, batch_id


def make_metadata(*, domain: str, source: str, batch_id: str, raw_object_key: str,
                  raw_filename: str, raw_bytes: bytes, content_type: str,
                  source_url: str | None, source_type: str,
                  extra: dict[str, Any] | None = None) -> dict[str, Any]:
    now = utc_now()
    metadata: dict[str, Any] = {
        "metadata_version": "1.0",
        "dataset_domain": domain,
        "source_name": source,
        "source_type": source_type,
        "layer": "bronze",
        "format": Path(raw_filename).suffix.lstrip(".").lower() or "binary",
        "content_type": content_type,
        "source_url": source_url,
        "ingested_at_utc": now.isoformat(),
        "ingestion_date": now.strftime("%Y-%m-%d"),
        "batch_id": batch_id,
        "raw_filename": Path(raw_filename).name,
        "raw_object_key": raw_object_key,
        "size_bytes": len(raw_bytes),
        "sha256": sha256_bytes(raw_bytes),
        "status": "success",
    }
    if extra:
        metadata["source_metadata"] = extra
    return metadata


def upload_bytes_and_metadata(*, raw_bytes: bytes, raw_filename: str, domain: str,
                              source: str, source_type: str,
                              source_url: str | None = None,
                              content_type: str | None = None,
                              batch_id: str | None = None,
                              extra_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    """Upload dữ liệu gốc và metadata.json vào MinIO; tạo batch riêng mỗi lần gọi."""
    client = get_minio_client()
    bucket = get_bucket_name()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f"Bucket '{bucket}' chưa tồn tại. Hãy tạo bucket trong MinIO Console trước.")

    prefix, batch_id = make_object_prefix(domain, source, batch_id=batch_id)
    filename = Path(raw_filename).name
    raw_key = f"{prefix}/raw/{filename}"
    metadata_key = f"{prefix}/metadata.json"
    detected_type = content_type or guess_content_type(filename)
    metadata = make_metadata(
        domain=domain, source=source, batch_id=batch_id, raw_object_key=raw_key,
        raw_filename=filename, raw_bytes=raw_bytes, content_type=detected_type,
        source_url=source_url, source_type=source_type, extra=extra_metadata,
    )

    client.put_object(bucket, raw_key, BytesIO(raw_bytes), len(raw_bytes), content_type=detected_type)
    metadata_bytes = json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(bucket, metadata_key, BytesIO(metadata_bytes), len(metadata_bytes), content_type="application/json")
    return {
        "bucket": bucket, "batch_id": batch_id, "raw_object_key": raw_key,
        "metadata_object_key": metadata_key, "size_bytes": len(raw_bytes),
        "sha256": metadata["sha256"],
    }
