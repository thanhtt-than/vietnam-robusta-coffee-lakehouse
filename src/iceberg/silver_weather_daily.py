"""Silver Weather daily from Iceberg Bronze (read-only dry run by default).

PowerShell:
  .\run_spark.ps1 -Script 'src/iceberg/silver_weather_daily.py'
  .\run_spark.ps1 -Script 'src/iceberg/silver_weather_daily.py' -ScriptArgs @('--write')

Never mutates MinIO raw or Bronze. source_series_id includes the source name,
actual coordinates, elevation and timezone, NOT the changing JSON request metadata.
"""
import sys
from pyspark.sql import SparkSession, Window, functions as F

SOURCE = 'lakehouse.bronze.weather_raw'
TARGET = 'lakehouse.silver.weather_daily'
KEY = ['source_series_id', 'observation_date']
NUMERICS = ['temperature_2m_mean', 'temperature_2m_min', 'temperature_2m_max',
            'rain_sum', 'precipitation_sum', 'et0_fao_evapotranspiration']


def get_json(raw_col, field):
    return F.get_json_object(F.col(raw_col), '$.' + field)


def field_from_raw(normal_name, csv_name):
    # Spark JSON path bracket quoting for source names with spaces/symbols.
    return F.coalesce(
        F.get_json_object('raw_record_json', '$.' + normal_name),
        F.get_json_object('raw_record_json', "$['" + csv_name + "']"),
    )


def province_for_key(col):
    x = F.lower(col)
    return (F.when(x.rlike(r'dak[_-]?lak|daklak'), F.lit('DAK_LAK'))
            .when(x.rlike(r'dak[_-]?nong|daknong'), F.lit('DAK_NONG'))
            .when(x.rlike(r'gia[_-]?lai|gialai'), F.lit('GIA_LAI'))
            .when(x.rlike(r'lam[_-]?dong|lamdong'), F.lit('LAM_DONG')))


def build_observations(spark):
    raw = spark.table(SOURCE)
    required = {'record_id', 'object_key', 'record_position', 'source',
                'record_type', 'source_context_json', 'raw_record_json'}
    missing = required - set(raw.columns)
    if missing:
        raise RuntimeError('Missing Weather Bronze columns: ' + repr(sorted(missing)))

    df = raw.select(
        'record_id', 'object_key', 'record_position', 'source', 'record_type',
        get_json('source_context_json', 'latitude').cast('double').alias('latitude'),
        get_json('source_context_json', 'longitude').cast('double').alias('longitude'),
        get_json('source_context_json', 'elevation').cast('double').alias('elevation_m'),
        get_json('source_context_json', 'timezone').alias('source_timezone'),
        F.to_date(get_json('raw_record_json', 'time')).alias('observation_date'),
        field_from_raw('weather_code', 'weather_code (wmo code)').cast('int').alias('weather_code'),
        *[field_from_raw(k, k + (' (°C)' if 'temperature' in k else ' (mm)')).cast('double').alias(k)
          for k in NUMERICS],
    )

    # Latitude/longitude can be rounded inconsistently across JSON/CSV sources;
    # source name and timezone intentionally keep their series separate.
    df = df.withColumn('province', province_for_key(F.col('object_key')))
    df = df.withColumn('source_series_id', F.concat_ws('|',
        F.col('source'), F.col('province'),
        F.format_string('%.6f', F.col('latitude')),
        F.format_string('%.6f', F.col('longitude')),
        F.format_string('%.1f', F.col('elevation_m')),
        F.coalesce(F.col('source_timezone'), F.lit('UNKNOWN_TZ')),
    ))

    nonnegative = (F.col('rain_sum') >= 0) & (F.col('precipitation_sum') >= 0) & (F.col('et0_fao_evapotranspiration') >= 0)
    numeric_valid = F.lit(True)
    for k in NUMERICS:
        numeric_valid = numeric_valid & F.col(k).isNotNull() & (~F.isnan(F.col(k)))
    # Do not reject real extreme weather solely on a speculative geographic threshold.
    valid = (
        F.col('record_id').isNotNull() & F.col('object_key').isNotNull()
        & F.col('province').isNotNull() & F.col('source').isNotNull()
        & F.col('record_type').isin('open_meteo_csv_daily', 'open_meteo_json_daily')
        & F.col('observation_date').isNotNull()
        & F.col('latitude').between(-90, 90)
        & F.col('longitude').between(-180, 180)
        & F.col('elevation_m').isNotNull()
        & F.col('weather_code').between(0, 99)
        & numeric_valid & nonnegative
        & (F.col('temperature_2m_min') <= F.col('temperature_2m_mean'))
        & (F.col('temperature_2m_mean') <= F.col('temperature_2m_max'))
    )
    return df.withColumn('is_valid_weather', valid)


def canonicalize(valid):
    metric_cols = ['weather_code'] + NUMERICS
    # Use a metric signature to distinguish repeated raw copies from genuine conflicts.
    signature = F.to_json(F.struct(*[F.col(k) for k in metric_cols]))
    d = valid.withColumn('_metric_signature', signature)
    agg = d.groupBy(*KEY).agg(
        F.count('*').cast('long').alias('raw_observation_count'),
        F.countDistinct('_metric_signature').cast('long').alias('distinct_measurement_count'),
    )
    window = Window.partitionBy(*KEY).orderBy(
        # For conflicting reports, deterministically choose a source record;
        # DO NOT claim this is necessarily the most accurate weather measurement.
        F.col('object_key').asc(), F.col('record_position').asc(), F.col('record_id').asc()
    )
    selected = d.withColumn('_rn', F.row_number().over(window)).filter('_rn = 1')
    final = selected.join(agg, KEY, 'inner').select(
        'source_series_id', 'source', 'province', 'observation_date',
        'latitude', 'longitude', 'elevation_m', 'source_timezone',
        'weather_code', *NUMERICS, 'raw_observation_count', 'distinct_measurement_count',
        F.when(F.col('distinct_measurement_count') > 1, F.lit('MEASUREMENT_CONFLICT_REVIEW'))
         .otherwise(F.lit('PASS')).alias('quality_status'),
        F.col('record_id').alias('selected_bronze_record_id'),
        F.col('object_key').alias('selected_bronze_object_key'),
        F.col('record_position').alias('selected_bronze_record_position'),
    )
    return final.withColumn('silver_processed_at', F.current_timestamp())


def run():
    flags = set(sys.argv[1:])
    if flags - {'--write'}:
        raise ValueError('Unknown arguments: ' + repr(flags - {'--write'}))
    write = '--write' in flags
    spark = (SparkSession.builder.appName('TLCN-Silver-Weather')
             .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
             .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
             .config('spark.sql.catalog.lakehouse.type', 'rest')
             .config('spark.sql.catalog.lakehouse.uri', 'http://nessie:19120/iceberg/main/')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        src = build_observations(spark).cache()
        total = src.count()
        print('WEATHER_SOURCE_TOTAL:', total, flush=True)
        if total != 156108:
            raise RuntimeError(f'Unexpected Bronze Weather row count {total}; review before write')
        src.groupBy('source', 'record_type', 'is_valid_weather').count().orderBy('source', 'record_type').show(30, False)
        valid = src.filter('is_valid_weather').cache()
        accepted = valid.count()
        print('WEATHER_VALID_OBSERVATIONS:', accepted, flush=True)
        print('WEATHER_EXCLUDED_OR_INVALID:', total - accepted, flush=True)
        if accepted == 0:
            raise RuntimeError('No valid Weather observations')
        if accepted != total:
            print('WEATHER_INVALID_EXAMPLES:', flush=True)
            src.filter('NOT is_valid_weather').select('source', 'object_key', 'record_id', 'observation_date', 'province').show(15, False)
        canonical = canonicalize(valid).cache()
        rows = canonical.count()
        distinct_keys = canonical.select(*KEY).distinct().count()
        stations = canonical.select('source_series_id').distinct().count()
        print('WEATHER_CANONICAL_ROWS:', rows, flush=True)
        print('WEATHER_STATION_SERIES:', stations, flush=True)
        print('WEATHER_UNIQUE_KEYS:', distinct_keys, flush=True)
        canonical.groupBy('source', 'province', 'quality_status').count().orderBy('source', 'province', 'quality_status').show(50, False)
        print('WEATHER_CONFLICT_EXAMPLES:', flush=True)
        canonical.filter('distinct_measurement_count > 1').select('source_series_id', 'observation_date', 'raw_observation_count', 'distinct_measurement_count').show(20, False)
        print('WEATHER_STATION_EXAMPLES:', flush=True)
        canonical.groupBy('source_series_id', 'source', 'province').agg(F.count('*').alias('days'), F.min('observation_date').alias('first_day'), F.max('observation_date').alias('last_day')).orderBy('source_series_id').show(40, False)
        if rows <= 0 or rows > accepted or rows != distinct_keys:
            raise RuntimeError('Invalid canonical Weather counts / keys')
        if canonical.filter('selected_bronze_record_id IS NULL OR selected_bronze_object_key IS NULL').limit(1).count():
            raise RuntimeError('Missing Bronze lineage')
        if not write:
            print('SILVER_WEATHER_DRY_RUN_PASSED', flush=True)
            print('NO_SILVER_TABLE_WRITTEN', flush=True)
            return
        if accepted != total:
            raise RuntimeError('Weather contains invalid observations; review dry-run before using --write')
        spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.silver')
        spark.sql(f'''CREATE TABLE IF NOT EXISTS {TARGET} (
            source_series_id STRING, source STRING, province STRING, observation_date DATE,
            latitude DOUBLE, longitude DOUBLE, elevation_m DOUBLE, source_timezone STRING,
            weather_code INT, temperature_2m_mean DOUBLE, temperature_2m_min DOUBLE,
            temperature_2m_max DOUBLE, rain_sum DOUBLE, precipitation_sum DOUBLE,
            et0_fao_evapotranspiration DOUBLE, raw_observation_count BIGINT,
            distinct_measurement_count BIGINT, quality_status STRING,
            selected_bronze_record_id STRING, selected_bronze_object_key STRING,
            selected_bronze_record_position BIGINT, silver_processed_at TIMESTAMP
        ) USING iceberg''')
        canonical.createOrReplaceTempView('weather_silver_stage')
        spark.sql(f'''MERGE INTO {TARGET} t USING weather_silver_stage s
            ON t.source_series_id=s.source_series_id AND t.observation_date=s.observation_date
            WHEN MATCHED THEN UPDATE SET
                t.source=s.source, t.province=s.province, t.latitude=s.latitude,
                t.longitude=s.longitude, t.elevation_m=s.elevation_m,
                t.source_timezone=s.source_timezone, t.weather_code=s.weather_code,
                t.temperature_2m_mean=s.temperature_2m_mean,
                t.temperature_2m_min=s.temperature_2m_min,
                t.temperature_2m_max=s.temperature_2m_max,
                t.rain_sum=s.rain_sum, t.precipitation_sum=s.precipitation_sum,
                t.et0_fao_evapotranspiration=s.et0_fao_evapotranspiration,
                t.raw_observation_count=s.raw_observation_count,
                t.distinct_measurement_count=s.distinct_measurement_count,
                t.quality_status=s.quality_status,
                t.selected_bronze_record_id=s.selected_bronze_record_id,
                t.selected_bronze_object_key=s.selected_bronze_object_key,
                t.selected_bronze_record_position=s.selected_bronze_record_position
            WHEN NOT MATCHED THEN INSERT *''')
        existing = spark.table(TARGET)
        written = existing.count()
        print('SILVER_WEATHER_TOTAL:', written, flush=True)
        if written != rows or existing.select(*KEY).distinct().count() != rows:
            raise RuntimeError('Weather row/key count mismatch; inspect table (do not drop)')
        print('SILVER_WEATHER_ICEBERG_PASSED', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    run()
