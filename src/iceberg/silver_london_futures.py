"""TLCN: Bronze Iceberg -> Silver London coffee futures, dry-run by default.

Run:
  .\run_spark.ps1 -Script 'src/iceberg/silver_london_futures.py'
  .\run_spark.ps1 -Script 'src/iceberg/silver_london_futures.py' -ScriptArgs @('--write')

No MinIO staging.  Never modifies Bronze.  Keeps problematic OHLC/volume rows
with independent quality flags; does not invent market holiday observations.
The quotation unit of the historical CSV is intentionally left unspecified.
"""
import sys
from pyspark.sql import SparkSession, functions as F, Window

SOURCE = 'lakehouse.bronze.london_futures_raw'
TARGET = 'lakehouse.silver.london_futures_daily'
KEY = ['trade_date']


def json_text(field):
    # Map access supports Unicode keys and spaces without fragile JSONPath quoting.
    return F.element_at(F.col('_payload'), F.lit(field))


def parse_decimal_string(column):
    """'4,781.00' -> decimal; bad tokens -> NULL (never silently zero)."""
    clean = F.regexp_replace(F.trim(column), ',', '')
    return F.when(clean.rlike(r'^[+-]?[0-9]+(?:\.[0-9]+)?$'),
                  clean.cast('decimal(18,2)')).otherwise(F.lit(None).cast('decimal(18,2)'))


def load_and_normalize(spark):
    bronze = spark.table(SOURCE)
    required = {'record_id', 'object_key', 'record_position', 'source', 'object_sha256', 'raw_record_json'}
    if not required.issubset(bronze.columns):
        raise RuntimeError(f'Bronze missing columns: {sorted(required - set(bronze.columns))}')

    df = bronze.withColumn('_payload', F.from_json('raw_record_json', 'MAP<STRING,STRING>'))
    df = df.select(
        'record_id', 'object_key', 'record_position', 'source', 'object_sha256', 'raw_record_json',
        json_text('Ngày').alias('_date'),
        json_text('Lần cuối').alias('_last'),
        json_text('Mở').alias('_open'),
        json_text('Cao').alias('_high'),
        json_text('Thấp').alias('_low'),
        json_text('KL').alias('_volume'),
        json_text('% Thay đổi').alias('_change'))

    # '7.85K' -> 7850; '2.3M' -> 2300000; '-' or empty -> NULL.
    vol = F.upper(F.regexp_replace(F.trim(F.col('_volume')), ',', ''))
    base = F.regexp_extract(vol, r'^([0-9]+(?:\.[0-9]+)?)(?:[KMB])?$', 1)
    suffix = F.regexp_extract(vol, r'^([0-9]+(?:\.[0-9]+)?)([KMB])?$', 2)
    multiplier = (F.when(suffix == 'K', F.lit(1000))
                    .when(suffix == 'M', F.lit(1000000))
                    .when(suffix == 'B', F.lit(1000000000))
                    .otherwise(F.lit(1)))
    change_str = F.regexp_replace(F.trim(F.col('_change')), '%', '')
    change_str = F.regexp_replace(change_str, ',', '')
    df = (df
        .withColumn('trade_date', F.to_date('_date', 'dd/MM/yyyy'))
        .withColumn('last_price', parse_decimal_string(F.col('_last')))
        .withColumn('open_price', parse_decimal_string(F.col('_open')))
        .withColumn('high_price', parse_decimal_string(F.col('_high')))
        .withColumn('low_price', parse_decimal_string(F.col('_low')))
        .withColumn('volume_normalized',
            F.when(base != '', (base.cast('double') * multiplier).cast('decimal(20,2)'))
             .otherwise(F.lit(None).cast('decimal(20,2)')))
        .withColumn('change_percent',
            F.when(change_str.rlike(r'^[+-]?[0-9]+(?:\.[0-9]+)?$'),
                   change_str.cast('decimal(12,3)'))
             .otherwise(F.lit(None).cast('decimal(12,3)')))
    )
    df = (df
        .withColumn('ohlc_review',
            F.col('last_price').isNull() | F.col('open_price').isNull() |
            F.col('high_price').isNull() | F.col('low_price').isNull() |
            (F.col('last_price') <= 0) | (F.col('open_price') <= 0) |
            (F.col('high_price') <= 0) | (F.col('low_price') <= 0) |
            (F.col('high_price') < F.greatest('last_price', 'open_price', 'low_price')) |
            (F.col('low_price') > F.least('last_price', 'open_price', 'high_price')))
        .withColumn('volume_missing', F.col('volume_normalized').isNull())
        .withColumn('volume_invalid', F.col('volume_normalized') < 0)
        .withColumn('change_missing', F.col('change_percent').isNull())
    )
    return df


def canonicalize(src):
    counts = src.groupBy(*KEY).agg(
        F.count('*').alias('raw_observation_count'),
        F.countDistinct('raw_record_json').alias('distinct_raw_version_count'),
        F.countDistinct('object_sha256').alias('distinct_object_sha_count'))
    priority = Window.partitionBy(*KEY).orderBy(
        F.when(F.col('source') == 'historical_csv', 0).otherwise(1),
        F.col('object_key').asc(), F.col('record_position').asc(), F.col('record_id').asc())
    chosen = src.withColumn('_rn', F.row_number().over(priority)).filter('_rn = 1').drop('_rn')
    out = chosen.join(counts, KEY, 'inner')
    out = (out
        .withColumn('raw_version_conflict', F.col('distinct_raw_version_count') > 1)
        .withColumn('quality_status',
            F.when(F.col('distinct_raw_version_count') > 1, 'SOURCE_VERSION_CONFLICT_REVIEW')
             .when(F.col('ohlc_review'), 'OHLC_REVIEW')
             .when(F.col('volume_missing') | F.col('volume_invalid'), 'VOLUME_REVIEW')
             .when(F.col('change_missing'), 'CHANGE_PCT_REVIEW')
             .otherwise('PASS'))
        .withColumn('silver_processed_at', F.current_timestamp()))
    return out.select(
        'trade_date', 'last_price', 'open_price', 'high_price', 'low_price',
        'volume_normalized', 'change_percent', 'ohlc_review', 'volume_missing',
        'volume_invalid', 'change_missing', 'raw_version_conflict',
        'quality_status', 'raw_observation_count', 'distinct_raw_version_count',
        'distinct_object_sha_count',
        F.col('record_id').alias('selected_bronze_record_id'),
        F.col('object_key').alias('selected_bronze_object_key'),
        F.col('record_position').alias('selected_bronze_record_position'),
        F.col('source').alias('selected_source'), 'silver_processed_at')


def main():
    write = '--write' in sys.argv[1:]
    spark = (SparkSession.builder.appName('TLCN-Silver-London-Futures')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.lakehouse.type', 'rest')
        .config('spark.sql.catalog.lakehouse.uri', 'http://nessie:19120/iceberg/main/')
        .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        src = load_and_normalize(spark).cache()
        total = src.count()
        print('LONDON_SOURCE_TOTAL:', total, flush=True)
        src.groupBy('source').agg(F.count('*').alias('rows'),
                                  F.countDistinct('object_key').alias('source_objects'),
                                  F.countDistinct('object_sha256').alias('sha256_values')).show(truncate=False)
        invalid_dates = src.filter('trade_date IS NULL').count()
        print('LONDON_INVALID_DATES:', invalid_dates, flush=True)
        if total != 9510 or invalid_dates:
            raise RuntimeError('Unexpected Bronze record count or invalid date; review before writing.')
        canonical = canonicalize(src).cache()
        n = canonical.count()
        print('LONDON_CANONICAL_ROWS:', n, flush=True)
        print('LONDON_UNIQUE_DATES:', canonical.select('trade_date').distinct().count(), flush=True)
        print('LONDON_OHLC_REVIEW:', canonical.filter('ohlc_review').count(), flush=True)
        print('LONDON_VOLUME_MISSING:', canonical.filter('volume_missing').count(), flush=True)
        print('LONDON_SOURCE_VERSION_CONFLICTS:', canonical.filter('raw_version_conflict').count(), flush=True)
        canonical.groupBy('quality_status').count().orderBy('quality_status').show(truncate=False)
        print('LONDON_REVIEW_EXAMPLES:', flush=True)
        canonical.filter('ohlc_review OR volume_missing OR raw_version_conflict').select(
            'trade_date', 'last_price', 'open_price', 'high_price', 'low_price',
            'volume_normalized', 'ohlc_review', 'volume_missing', 'quality_status').orderBy('trade_date').show(16, truncate=False)
        if n != 4755 or n != canonical.select('trade_date').distinct().count():
            raise RuntimeError('Unexpected canonical count/duplicate date; check source before publishing.')
        if canonical.filter('selected_bronze_record_id IS NULL OR selected_bronze_object_key IS NULL').limit(1).count():
            raise RuntimeError('Missing Bronze lineage')
        if not write:
            print('SILVER_LONDON_DRY_RUN_PASSED', flush=True)
            print('NO_SILVER_TABLE_WRITTEN', flush=True)
            return
        spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.silver')
        spark.sql(f'''CREATE TABLE IF NOT EXISTS {TARGET} (
            trade_date DATE, last_price DECIMAL(18,2), open_price DECIMAL(18,2),
            high_price DECIMAL(18,2), low_price DECIMAL(18,2),
            volume_normalized DECIMAL(20,2), change_percent DECIMAL(12,3),
            ohlc_review BOOLEAN, volume_missing BOOLEAN, volume_invalid BOOLEAN,
            change_missing BOOLEAN, raw_version_conflict BOOLEAN, quality_status STRING,
            raw_observation_count BIGINT, distinct_raw_version_count BIGINT,
            distinct_object_sha_count BIGINT, selected_bronze_record_id STRING,
            selected_bronze_object_key STRING, selected_bronze_record_position BIGINT,
            selected_source STRING, silver_processed_at TIMESTAMP
        ) USING iceberg''')
        canonical.createOrReplaceTempView('london_silver_stage')
        spark.sql(f'''MERGE INTO {TARGET} t USING london_silver_stage s
            ON t.trade_date = s.trade_date
            WHEN MATCHED THEN UPDATE SET
                t.last_price=s.last_price, t.open_price=s.open_price,
                t.high_price=s.high_price, t.low_price=s.low_price,
                t.volume_normalized=s.volume_normalized,
                t.change_percent=s.change_percent, t.ohlc_review=s.ohlc_review,
                t.volume_missing=s.volume_missing, t.volume_invalid=s.volume_invalid,
                t.change_missing=s.change_missing,
                t.raw_version_conflict=s.raw_version_conflict,
                t.quality_status=s.quality_status,
                t.raw_observation_count=s.raw_observation_count,
                t.distinct_raw_version_count=s.distinct_raw_version_count,
                t.distinct_object_sha_count=s.distinct_object_sha_count,
                t.selected_bronze_record_id=s.selected_bronze_record_id,
                t.selected_bronze_object_key=s.selected_bronze_object_key,
                t.selected_bronze_record_position=s.selected_bronze_record_position,
                t.selected_source=s.selected_source
            WHEN NOT MATCHED THEN INSERT *''')
        current = spark.table(TARGET)
        final_n = current.count()
        print('SILVER_LONDON_TOTAL:', final_n, flush=True)
        if final_n != n or final_n != current.select('trade_date').distinct().count():
            raise RuntimeError('London Silver count/key mismatch; inspect before rerunning (do not DROP).')
        print('SILVER_LONDON_ICEBERG_PASSED', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
