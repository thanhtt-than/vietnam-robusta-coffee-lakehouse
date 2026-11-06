"""Direct read-only MinIO S3 access for Spark-driver Bronze ingest.

Uses Python standard library and AWS Signature V4; no Hadoop S3A, boto3,
MinIO SDK, or local staging files are required inside the Spark container.
Existing Iceberg REST+S3FileIO setup remains unchanged.

NOTE: This is driver-side ingestion, appropriate for the current small Bronze
sources. For massive object collections, migrate to Hadoop S3A distributed reads.
"""
from __future__ import annotations

import csv
import hashlib
import hmac
import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone


class MinioDirect:
    def __init__(self):
        self.endpoint = os.environ.get("MINIO_S3_INTERNAL_ENDPOINT", "http://minio:9000").rstrip("/")
        self.bucket = os.environ.get("MINIO_BUCKET", "coffee-lakehouse")
        self.key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        self.secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        parsed = urllib.parse.urlsplit(self.endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path not in {"", "/"}:
            raise RuntimeError("MINIO_S3_INTERNAL_ENDPOINT must be http(s)://host:port")
        if not self.key or not self.secret:
            raise RuntimeError("Spark is missing AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables")
        self.host = parsed.netloc

    def _request(self, object_key: str = "", params: dict | None = None) -> bytes:
        # S3 path-style. Percent-encode Vietnamese object keys (e.g. London Futures).
        encoded_path = urllib.parse.quote("/" + self.bucket + ("/" + object_key if object_key else ""), safe="/-_.~")
        query = urllib.parse.urlencode(sorted((params or {}).items()), quote_via=urllib.parse.quote, safe="-_.~")
        url = self.endpoint + encoded_path + (("?" + query) if query else "")
        payload_hash = hashlib.sha256(b"").hexdigest()

        for attempt in range(4):
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            day = timestamp[:8]
            headers = {
                "host": self.host,
                "x-amz-content-sha256": payload_hash,
                "x-amz-date": timestamp,
            }
            canonical_headers = "".join(f"{k}:{v}\n" for k, v in sorted(headers.items()))
            signed_headers = ";".join(sorted(headers))
            canonical_request = "\n".join([
                "GET", encoded_path, query, canonical_headers, signed_headers, payload_hash
            ])
            scope = f"{day}/{self.region}/s3/aws4_request"
            string_to_sign = "\n".join([
                "AWS4-HMAC-SHA256", timestamp, scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ])

            def mac(k: bytes, msg: str) -> bytes:
                return hmac.new(k, msg.encode("utf-8"), hashlib.sha256).digest()

            sign_key = mac(mac(mac(mac(("AWS4" + self.secret).encode(), day), self.region), "s3"), "aws4_request")
            signature = hmac.new(sign_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
            headers["Authorization"] = (
                f"AWS4-HMAC-SHA256 Credential={self.key}/{scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            )
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=120) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                # No retries for authentication errors or missing objects.
                if exc.code not in (429, 500, 502, 503, 504) or attempt == 3:
                    msg = exc.read(500).decode("utf-8", errors="replace")
                    raise RuntimeError(f"MinIO HTTP {exc.code} on {object_key or 'LIST'}: {msg}") from exc
            except (urllib.error.URLError, TimeoutError):
                if attempt == 3:
                    raise
            time.sleep(2 ** attempt)
        raise RuntimeError("Unreachable MinIO retry state")

    def list_keys(self, prefix: str):
        token = None
        while True:
            params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
            if token:
                params["continuation-token"] = token
            root = ET.fromstring(self._request(params=params))
            for content in root.findall("{*}Contents"):
                key = content.findtext("{*}Key")
                if key:
                    yield key
            truncated = root.findtext("{*}IsTruncated", "false") == "true"
            if not truncated:
                break
            next_token = root.findtext("{*}NextContinuationToken")
            if not next_token or next_token == token:
                raise RuntimeError("MinIO list truncated without new continuation token")
            token = next_token

    def read(self, key: str) -> bytes:
        return self._request(object_key=key)


def csv_records(key: str, raw: bytes):
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    if not reader.fieldnames:
        raise ValueError(f"Missing CSV header: {key}")
    for row in reader:
        if None in row:
            raise ValueError(f"CSV has extra columns: {key}")
        yield row


def json_data_records(key: str, raw: bytes):
    payload = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise ValueError(f"Expected JSON data[]: {key}")
    for row in payload["data"]:
        if not isinstance(row, dict):
            raise ValueError(f"Non-object JSON data[] record: {key}")
        yield row


def json_root_list_records(key: str, raw: bytes):
    payload = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON root list: {key}")
    for row in payload:
        if not isinstance(row, dict):
            raise ValueError(f"Non-object JSON list record: {key}")
        yield row


def record_id_for(key: str, position: int) -> str:
    return hashlib.sha256(f"{key}\n{position}".encode("utf-8")).hexdigest()


def json_record(value: dict) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def spark_session(name: str):
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName(name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/iceberg/main/")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def merge_into_bronze(spark, table: str, schema, rows: list[tuple], temp_view: str, label: str):
    from pyspark.sql.functions import col
    if not rows:
        raise RuntimeError("No data records found; refusing empty ingestion")
    # 14k domestic rows fit in the current small test deployment. This method
    # avoids writing JSONL staging; source bytes are fetched by the Spark driver.
    df = spark.createDataFrame(rows, schema=schema).cache()
    try:
        total = df.count()
        unique = df.select("record_id").distinct().count()
        if unique != total:
            raise RuntimeError(f"Duplicate record IDs: total={total}, unique={unique}")
        required_nulls = col(schema.names[0]).isNull()
        for name in schema.names[1:]:
            required_nulls = required_nulls | col(name).isNull()
        if df.filter(required_nulls).limit(1).count():
            raise RuntimeError("Null required fields in Bronze input")
        print(f"SOURCE_RECORDS: {total}", flush=True)
        print(f"UNIQUE_RECORD_IDS: {unique}", flush=True)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        sql_types = {"string": "STRING", "bigint": "BIGINT"}
        definitions = ", ".join(f"{field.name} {sql_types[field.dataType.simpleString()]}" for field in schema.fields)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({definitions}) USING iceberg")
        target = spark.table(table)
        if set(target.columns) != set(schema.names):
            raise RuntimeError(f"Existing table schema differs: {table}. No changes were written.")
        comparison = df.alias("incoming").join(target.alias("stored"), on="record_id", how="inner")
        # Compare full logical payload and existing lineage. A source object
        # overwritten at the same key must not silently keep obsolete records.
        mismatch = None
        for c in schema.names:
            if c == "record_id":
                continue
            term = ~col(f"incoming.{c}").eqNullSafe(col(f"stored.{c}"))
            mismatch = term if mismatch is None else (mismatch | term)
        if comparison.filter(mismatch).limit(1).count():
            raise RuntimeError(f"Existing {table} row differs from current MinIO raw; stop and investigate")
        new_count = df.select("record_id").join(target.select("record_id"), on="record_id", how="left_anti").count()
        print(f"NEW_RECORDS: {new_count}", flush=True)
        if new_count:
            df.createOrReplaceTempView(temp_view)
            column_list = ", ".join(schema.names)
            source_list = ", ".join(f"source.{name}" for name in schema.names)
            spark.sql(f"""
                MERGE INTO {table} AS target
                USING {temp_view} AS source
                ON target.record_id = source.record_id
                WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({source_list})
            """)
        final_table = spark.table(table)
        final_count = final_table.count()
        matched = final_table.join(df.select("record_id"), on="record_id", how="inner").count()
        print(f"{label}_TOTAL: {final_count}", flush=True)
        print(f"SOURCE_RECORDS_PRESENT_IN_ICEBERG: {matched}", flush=True)
        if matched != total:
            raise RuntimeError(f"Only {matched}/{total} source records present in {table}")
        print(f"{label}_PASSED", flush=True)
    finally:
        df.unpersist()
