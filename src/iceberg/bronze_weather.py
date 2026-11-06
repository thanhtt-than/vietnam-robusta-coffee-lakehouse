"""MinIO weather raw objects -> Nessie Iceberg Bronze, directly from Spark driver.

Install in D:/TLCN/src/iceberg/ beside minio_direct.py.
Run from Windows: .\\run_spark.ps1 -Script 'src/iceberg/bronze_weather.py'

Preserves EVERY daily weather observation (including repeated snapshots) and
source coordinates; no local staging and no source object writes.
Open-Meteo CSV's leading station-metadata block is NOT a weather observation.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
from collections import Counter

from pyspark.sql.types import LongType, StringType, StructField, StructType
from minio_direct import MinioDirect, json_record, merge_into_bronze, record_id_for, spark_session

TABLE = "lakehouse.bronze.weather_raw"
PREFIX = "bronze/weather/"
BATCH_ROWS = 12000  # bound driver's memory and allow safe restart after a partial run
SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("record_position", LongType(), False),
    StructField("domain", StringType(), False),
    StructField("source", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("object_sha256", StringType(), False),
    StructField("raw_record_json", StringType(), False),
    StructField("source_context_json", StringType(), False),
])


def parse_weather_csv(key: str, raw: bytes):
    """Parse Open-Meteo download CSV with metadata header, blank, daily header."""
    text = raw.decode("utf-8-sig")
    lines = text.splitlines()
    if len(lines) < 4:
        raise ValueError(f"Weather CSV too short: {key}")
    meta_names = next(csv.reader([lines[0]]))
    meta_values = next(csv.reader([lines[1]]))
    if len(meta_names) != len(meta_values) or "latitude" not in meta_names:
        raise ValueError(f"Unrecognized Open-Meteo CSV station metadata: {key}")
    station_meta = dict(zip(meta_names, meta_values))
    header_index = next((i for i, line in enumerate(lines[2:], start=2)
                         if line.startswith("time,") or line.startswith('"time",')), None)
    if header_index is None:
        raise ValueError(f"Missing Open-Meteo daily header: {key}")
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_index:]) + "\n", newline=""))
    if not reader.fieldnames or reader.fieldnames[0] != "time":
        raise ValueError(f"Invalid weather daily header: {key}")
    for position, record in enumerate(reader, start=1):
        if None in record or not record.get("time"):
            raise ValueError(f"Malformed weather CSV daily row {position}: {key}")
        yield position, record, station_meta


def parse_weather_daily_json(key: str, raw: bytes):
    """Extract each day while keeping top-level coordinates and daily units."""
    payload = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"Weather JSON root not an object: {key}")
    daily = payload.get("daily")
    if not isinstance(daily, dict) or not isinstance(daily.get("time"), list):
        raise ValueError(f"Weather JSON has no daily.time[]: {key}")
    arrays = {name: values for name, values in daily.items() if isinstance(values, list)}
    expected = len(daily["time"])
    for name, values in arrays.items():
        if len(values) != expected:
            raise ValueError(f"Weather daily array {name} has length {len(values)} != {expected}: {key}")
    context = {k: v for k, v in payload.items() if k not in {"daily", "hourly", "minutely_15"}}
    for position in range(1, expected + 1):
        record = {name: values[position - 1] for name, values in arrays.items()}
        if not record.get("time"):
            raise ValueError(f"Weather JSON missing date at row {position}: {key}")
        yield position, record, context


def main():
    minio = MinioDirect()
    keys = sorted(k for k in minio.list_keys(PREFIX) if k.lower().endswith((".csv", ".json"))
                  and not k.endswith("/metadata.json"))
    if not keys:
        raise RuntimeError("No weather raw objects found in MinIO")
    print(f"WEATHER_SOURCE_OBJECTS: {len(keys)}", flush=True)
    spark = spark_session("TLCN-Bronze-Weather-Direct")
    counts = Counter()
    batches = 0
    buf = []
    try:
        for idx, key in enumerate(keys, start=1):
            raw = minio.read(key)
            digest = hashlib.sha256(raw).hexdigest()
            source = next((piece.split("=", 1)[1] for piece in key.split("/")
                           if piece.startswith("source=")), "legacy_unknown")
            if key.lower().endswith(".csv"):
                record_type = "open_meteo_csv_daily"
                iterator = parse_weather_csv(key, raw)
            else:
                record_type = "open_meteo_json_daily"
                iterator = parse_weather_daily_json(key, raw)
            per_object = 0
            for position, record, context in iterator:
                buf.append((record_id_for(key, position), key, position, "weather", source,
                            record_type, digest, json_record(record), json_record(context)))
                counts[record_type] += 1
                per_object += 1
                if len(buf) >= BATCH_ROWS:
                    batches += 1
                    print(f"COMMIT_BATCH: {batches} ({len(buf)} records)", flush=True)
                    merge_into_bronze(spark, TABLE, SCHEMA, buf, "weather_incoming",
                                      "BRONZE_WEATHER_BATCH")
                    buf.clear()
            if not per_object:
                counts["empty_objects"] += 1
            print(f"SOURCE_OBJECT: {idx}/{len(keys)} rows={per_object} type={record_type}", flush=True)
        if buf:
            batches += 1
            print(f"COMMIT_BATCH: {batches} ({len(buf)} records)", flush=True)
            merge_into_bronze(spark, TABLE, SCHEMA, buf, "weather_incoming", "BRONZE_WEATHER_BATCH")
            buf.clear()
        total_from_source = counts["open_meteo_csv_daily"] + counts["open_meteo_json_daily"]
        actual_total = spark.table(TABLE).count()
        print(f"WEATHER_SOURCE_RECORDS: {total_from_source}", flush=True)
        print(f"WEATHER_RECORDS_BY_TYPE: {dict(counts)}", flush=True)
        print(f"BRONZE_WEATHER_TOTAL: {actual_total}", flush=True)
        if actual_total != total_from_source:
            raise RuntimeError(f"Iceberg total {actual_total} differs from current source rows {total_from_source}. Check historical records / missing objects.")
        spark.sql(f"""SELECT source, record_type, count(*) AS records,
                      count(DISTINCT object_key) AS source_objects FROM {TABLE}
                      GROUP BY source, record_type ORDER BY source, record_type""").show(100, truncate=False)
        print("BRONZE_WEATHER_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
