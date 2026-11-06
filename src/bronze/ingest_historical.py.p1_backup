"""Batch ingestion for historical coffee data. Run: python -m src.bronze.ingest_historical --dry-run"""
from __future__ import annotations
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from minio.error import S3Error
from src.bronze.minio_client import get_bucket_name, get_minio_client
from src.bronze.metadata import upload_bytes_and_metadata

# Relative to D:\TLCN\data. API snapshots and website resources are intentionally excluded.
DATASETS = [
    ("area", "historical_csv", "coffee_area_vietnam_2005_2024.csv"),
    ("production", "historical_csv", "coffee_production_vietnam_2005_2024.csv"),
    ("trade", "historical_csv", "viet-nams-exports-to-world-by-importer_090111.csv"),
    ("domestic_price", "historical_csv", "vietnam_coffee_price_dataset.csv"),
    ("london_futures", "historical_csv", "Dữ liệu Lịch sử Hợp đồng Tương lai Cà phê London.csv"),
    ("weather", "open_meteo", "open-meteo-12.48N107.97E419m_DakNong.csv"),
    ("weather", "open_meteo", "open-meteo-12.83N108.13E481m_DaKLak.csv"),
    ("weather", "open_meteo", "open-meteo-14.09N109.18E336m_GiaLai.csv"),
    ("weather", "open_meteo", "open-meteo-20.91N106.60E5m_LamDong.csv"),
]

def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def prior_uploads(client, bucket):
    """Index only verified prior uploads made by this script (including older successful batches)."""
    index = set()
    for obj in client.list_objects(bucket, prefix="bronze/", recursive=True):
        if not obj.object_name.endswith("/metadata.json"):
            continue
        response = None
        try:
            response = client.get_object(bucket, obj.object_name)
            meta = json.loads(response.read())
            src = meta.get("source_metadata") or {}
            if meta.get("status") == "success" and src.get("ingestion_program") == "ingest_historical_v1":
                key = meta.get("raw_object_key")
                if key and client.stat_object(bucket, key).size == meta.get("size_bytes"):
                    index.add((meta.get("dataset_domain"), meta.get("source_name"), src.get("relative_path"), meta.get("sha256")))
        except (S3Error, ValueError, KeyError, TypeError) as exc:
            print(f"[WARN] Cannot index {obj.object_name}: {exc}")
        finally:
            if response is not None:
                response.close()
                response.release_conn()
    return index

def verify_download(client, bucket, key, expected):
    h = hashlib.sha256()
    response = client.get_object(bucket, key)
    try:
        for block in response.stream(1024 * 1024):
            h.update(block)
    finally:
        response.close()
        response.release_conn()
    if h.hexdigest() != expected:
        raise RuntimeError(f"Checksum mismatch: {key}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=r"D:\TLCN\data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    root = Path(args.data_dir)
    client = get_minio_client()
    bucket = get_bucket_name()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f"Bucket not found: {bucket}")
    known = prior_uploads(client, bucket)
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    records = []
    for domain, source, relative in DATASETS:
        path = root / relative
        entry = {"domain": domain, "source": source, "file": str(path), "relative_path": relative}
        try:
            if not path.is_file():
                raise FileNotFoundError(path)
            data = path.read_bytes()
            if not data:
                raise ValueError("Empty file")
            sha = digest(data)
            entry.update(size_bytes=len(data), sha256=sha)
            identity = (domain, source, relative, sha)
            if identity in known:
                entry["status"] = "SKIPPED"
            elif args.dry_run:
                entry["status"] = "WOULD_UPLOAD"
            else:
                result = upload_bytes_and_metadata(
                    raw_bytes=data,
                    raw_filename=path.name,
                    domain=domain,
                    source=source,
                    source_type="local_file",
                    extra_metadata={
                        "ingestion_program": "ingest_historical_v1",
                        "relative_path": relative,
                        "run_batch_id": batch_id,
                    },
                )
                verify_download(client, bucket, result["raw_object_key"], sha)
                entry.update(status="SUCCESS", object_key=result["raw_object_key"])
                known.add(identity)
        except Exception as exc:
            entry.update(status="FAILED", error=str(exc))
        print(f"[{entry['status']}] {relative}")
        records.append(entry)
    report = {"batch_id": batch_id, "bucket": bucket, "dry_run": args.dry_run, "files": records}
    report_dir = Path("reports") / "bronze"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"historical_{batch_id}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    from collections import Counter
    print("Summary:", dict(Counter(r["status"] for r in records)))
    print("Report:", report_path)
    if any(r["status"] == "FAILED" for r in records):
        raise SystemExit(1)

if __name__ == "__main__":
    main()
