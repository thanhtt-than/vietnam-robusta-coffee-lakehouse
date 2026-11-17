import os
import boto3
from pathlib import Path
from botocore.config import Config
from botocore.exceptions import ClientError

env = {}
for line in Path(os.environ["TLCN_ENV_PATH"]).read_text(
    encoding="utf-8-sig"
).splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k.strip()] = v.strip().strip('"').strip("'")

cfg = Config(s3={"addressing_style": "path"})

source = boto3.client(
    "s3",
    endpoint_url="http://localhost:9100",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=cfg
)

destination = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=env["MINIO_ACCESS_KEY"],
    aws_secret_access_key=env["MINIO_SECRET_KEY"],
    config=cfg
)

bucket = env["MINIO_BUCKET"]
prefix = "iceberg-warehouse/gold/"

copied = 0
skipped = 0

for page in source.get_paginator("list_objects_v2").paginate(
    Bucket=bucket, Prefix=prefix
):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        folder = key[len(prefix):].split("/")[0]

        if not folder.startswith(("dim_", "fact_")):
            continue

        try:
            existing = destination.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
        else:
            if existing["ContentLength"] != obj["Size"]:
                raise RuntimeError(f"Existing object size mismatch: {key}")
            skipped += 1
            continue

        data = source.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(data) != obj["Size"]:
            raise RuntimeError(f"Source object size mismatch: {key}")

        destination.put_object(
            Bucket=bucket, Key=key, Body=data
        )

        actual = destination.head_object(Bucket=bucket, Key=key)
        if actual["ContentLength"] != len(data):
            raise RuntimeError(f"Copy verification failed: {key}")

        copied += 1

print("OBJECTS_COPIED:", copied)
print("OBJECTS_SKIPPED:", skipped)
print("TOTAL_PROCESSED:", copied + skipped)
print("GOLD_COPY_COMPLETED")
