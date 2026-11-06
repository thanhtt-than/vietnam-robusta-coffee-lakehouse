from pathlib import Path
from pyspark.sql import SparkSession

output = Path("output/spark_io_test").resolve()

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("SparkIOTest")
    .getOrCreate()
)

try:
    df = spark.createDataFrame(
        [
            (1, "VICOFA"),
            (2, "BBW_COFFEE"),
            (3, "OPEN_METEO"),
        ],
        ["id", "source"]
    )

    df.write.mode("overwrite").parquet(str(output))

    result = spark.read.parquet(str(output))

    print("=== SPARK IO TEST ===")
    print("Rows:", result.count())
    result.show()

    assert result.count() == 3
    print("SPARK IO TEST: SUCCESS")

except Exception as exc:
    print("SPARK IO TEST: FAILED")
    print(type(exc).__name__, str(exc))
    raise

finally:
    spark.stop()