import os
import json
import boto3
from pathlib import Path
from collections import defaultdict
from botocore.config import Config
from pyiceberg.catalog import load_catalog

env = {}
for line in Path(os.environ["TLCN_ENV_PATH"]).read_text(
    encoding="utf-8-sig"
).splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k.strip()] = v.strip().strip('"').strip("'")

os.environ["AWS_ACCESS_KEY_ID"] = env["MINIO_ACCESS_KEY"]
os.environ["AWS_SECRET_ACCESS_KEY"] = env["MINIO_SECRET_KEY"]
os.environ["AWS_REGION"] = "us-east-1"

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=env["MINIO_ACCESS_KEY"],
    aws_secret_access_key=env["MINIO_SECRET_KEY"],
    config=Config(s3={"addressing_style": "path"})
)

catalog = load_catalog(
    "lakehouse",
    type="rest",
    uri="http://localhost:19120/iceberg/",
    prefix="main",
    **{
        "s3.endpoint": "http://localhost:9000",
        "s3.path-style-access": "true",
        "s3.access-key-id": env["MINIO_ACCESS_KEY"],
        "s3.secret-access-key": env["MINIO_SECRET_KEY"],
    }
)

bucket = env["MINIO_BUCKET"]
prefix = "iceberg-warehouse/gold/"
metadata_by_table = defaultdict(list)

for page in s3.get_paginator("list_objects_v2").paginate(
    Bucket=bucket,
    Prefix=prefix
):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        folder = key[len(prefix):].split("/")[0]

        if not folder.startswith(("dim_", "fact_")):
            continue
        if not key.endswith(".metadata.json"):
            continue

        metadata = json.loads(
            s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        )

        snapshot_id = metadata.get("current-snapshot-id")

        if snapshot_id not in (-1, None):
            table_name = folder.rsplit("_", 1)[0]
            metadata_by_table[table_name].append(
                f"s3://{bucket}/{key}"
            )

if len(metadata_by_table) != 18:
    raise RuntimeError(
        f"Expected 18 tables, found {len(metadata_by_table)}"
    )

for name, locations in metadata_by_table.items():
    if len(locations) != 1:
        raise RuntimeError(
            f"Ambiguous metadata for {name}: {len(locations)}"
        )

existing = {
    name[-1]
    for name in catalog.list_tables("gold")
}

registered = 0
skipped = 0

for name in sorted(metadata_by_table):
    if name in existing:
        print("SKIPPED_EXISTING:", name)
        skipped += 1
        continue

    location = metadata_by_table[name][0]

    table = catalog.register_table(
        ("gold", name),
        metadata_location=location,
        overwrite=False
    )

    print("REGISTERED:", ".".join(table.name()))
    registered += 1

print("TABLES_REGISTERED:", registered)
print("TABLES_SKIPPED:", skipped)
print("GOLD_WAREHOUSE_REGISTRATION_COMPLETED")
