import sys
import json

sys.path.insert(0, "/workspace/src/iceberg")
from minio_direct import spark_session

spark = spark_session("Inspect-IMF-Exchange-Series")

try:
    rows = (
        spark.table("lakehouse.bronze.exchange_rate_raw")
        .filter("source = 'imf_exchange_rate'")
        .select("raw_record_json")
        .collect()
    )

    print("\n=== IMF SERIES INVENTORY ===", flush=True)

    for index, row in enumerate(rows, 1):
        record = json.loads(row.raw_record_json)

        periods = [
            key for key, value in record.items()
            if key.startswith("20") and value not in (None, "")
        ]

        print(
            f"{index}. "
            f"SERIES={record.get('SERIES_CODE')} | "
            f"INDICATOR={record.get('INDICATOR')} | "
            f"TRANSFORMATION={record.get('TYPE_OF_TRANSFORMATION')} | "
            f"FREQUENCY={record.get('FREQUENCY')} | "
            f"PERIODS={len(periods)}",
            flush=True
        )

    print("IMF_SERIES_INSPECTION_PASSED", flush=True)

finally:
    spark.stop()
