from pyspark.sql import SparkSession, functions as F

CATALOG = "lakehouse"
BRONZE_NAMESPACE = "lakehouse.bronze"


def make_spark():
    spark = (
        SparkSession.builder
        .appName("TLCN-Inspect-Robusta-Stock-Bronze")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    spark = make_spark()
    try:
        print("===== BRONZE TABLES =====", flush=True)
        tables = spark.sql(f"SHOW TABLES IN {BRONZE_NAMESPACE}").collect()

        table_names = sorted([row.tableName for row in tables])
        for name in table_names:
            print(f"BRONZE_TABLE: {name}", flush=True)

        candidates = [
            name for name in table_names
            if "stock" in name.lower() or "robusta" in name.lower()
        ]

        if not candidates:
            print("NO_STOCK_NAMED_TABLE_FOUND", flush=True)
            print("===== TABLES WITH POSSIBLE STOCK SCHEMA =====", flush=True)
            candidates = []
            for name in table_names:
                df = spark.table(f"{BRONZE_NAMESPACE}.{name}")
                cols = {c.lower() for c in df.columns}
                if (
                    {"record_type"}.issubset(cols)
                    or {"portid", "lotswithvalcert"}.issubset(cols)
                    or {"commodity", "cutoffdate"}.issubset(cols)
                ):
                    candidates.append(name)

        if not candidates:
            raise RuntimeError(
                "Could not identify the Bronze Stock table. "
                "Send the BRONZE_TABLE list above."
            )

        for name in candidates:
            table = f"{BRONZE_NAMESPACE}.{name}"
            df = spark.table(table)

            print(f"\n===== STOCK TABLE: {table} =====", flush=True)
            print(f"TABLE_ROWS: {df.count()}", flush=True)
            print("===== SCHEMA =====", flush=True)
            df.printSchema()

            print("===== COLUMNS =====", flush=True)
            print(", ".join(df.columns), flush=True)

            if "record_type" in df.columns:
                print("===== RECORD TYPE COUNTS =====", flush=True)
                (
                    df.groupBy("record_type")
                    .count()
                    .orderBy("record_type")
                    .show(50, truncate=False)
                )

            print("===== SAMPLE ROWS =====", flush=True)
            df.show(8, truncate=False)

            if "record_type" in df.columns:
                for record_type in ("port_stock", "grand_total"):
                    subset = df.filter(F.col("record_type") == record_type)
                    if subset.limit(1).count():
                        print(f"===== SAMPLE: {record_type} =====", flush=True)
                        subset.show(3, truncate=False)

        print("\nBRONZE_ROBUSTA_STOCK_INSPECTION_PASSED", flush=True)
        print("READ_ONLY: TRUE", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
