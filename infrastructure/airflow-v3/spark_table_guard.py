"""Read-only Iceberg table existence check used by the TLCN Airflow DAG."""
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("table_name")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("TLCN-Airflow-Mart-Table-Guard")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/")
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)
try:
    if spark.catalog.tableExists(args.table_name):
        print(f"TLCN_TABLE_EXISTS: {args.table_name}", flush=True)
    else:
        print(f"TLCN_TABLE_MISSING: {args.table_name}", flush=True)
finally:
    spark.stop()
