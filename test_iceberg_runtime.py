
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TLCN-Iceberg-Runtime-Test")
    .master("local[2]")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .getOrCreate()
)

jvm = spark.sparkContext._jvm
loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()

classes = [
    "org.apache.iceberg.spark.SparkCatalog",
    "org.apache.iceberg.aws.s3.S3FileIO",
]

for class_name in classes:
    try:
        loader.loadClass(class_name)
        print(f"PASS: {class_name}")
    except Exception as exc:
        print(f"FAIL: {class_name}: {exc}")
        raise

spark.sql("SELECT 1 AS test_value").show()
spark.stop()
print("ICEBERG_RUNTIME_TEST_PASSED")
