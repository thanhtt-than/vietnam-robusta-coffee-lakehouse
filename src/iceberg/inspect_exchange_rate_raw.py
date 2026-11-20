import sys
import json

sys.path.insert(0, "/workspace/src/iceberg")
from minio_direct import spark_session

spark = spark_session("Inspect-Exchange-Rate-Bronze")

try:
    table = "lakehouse.bronze.exchange_rate_raw"

    print("=== BRONZE SOURCE SAMPLES ===", flush=True)

    sources = [
        "usd_vnd_csv",
        "eur_vnd_csv",
        "usd_vnd_excel",
        "eur_vnd_excel",
        "imf_exchange_rate",
    ]

    for source in sources:
        print(f"\n=== SOURCE: {source} ===", flush=True)

        samples = (
            spark.table(table)
            .filter(f"source = '{source}'")
            .orderBy("record_position")
            .select("raw_record_json")
            .limit(2)
            .collect()
        )

        for row in samples:
            record = json.loads(row.raw_record_json)

            if source == "imf_exchange_rate":
                print(
                    json.dumps(
                        {
                            "metadata": {
                                k: v for k, v in record.items()
                                if not k.startswith("20")
                            },
                            "first_periods": [
                                (k, v) for k, v in record.items()
                                if k.startswith("20")
                            ][:5],
                        },
                        ensure_ascii=False,
                    )
                )
            else:
                print(json.dumps(record, ensure_ascii=False))

finally:
    spark.stop()
