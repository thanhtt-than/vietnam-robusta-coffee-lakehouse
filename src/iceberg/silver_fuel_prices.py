"""TLCN Silver Fuel from existing Iceberg Bronze; dry-run by default.

Dry run: .\run_spark.ps1 -Script 'src/iceberg/silver_fuel_prices.py'
Write only after reviewing dry run: use a small runpy wrapper to set --write.
Never modifies Bronze or raw MinIO objects.

Bronze is incrementally ingested and may contain additional snapshots over time.
This job therefore does not enforce a historical fixed source-row count. It reports
the current source breakdown and validates supported fuel observations, canonical
business keys, and lineage before publishing Silver.
"""
import sys
from pyspark.sql import SparkSession, functions as F, Window

SOURCE = 'lakehouse.bronze.fuel_prices_raw'
TARGET = 'lakehouse.silver.fuel_price_observations'


def j(name):
    return F.get_json_object(F.col('raw_record_json'), '$.' + name)


def text_clean(expr):
    return F.upper(F.trim(expr))


def build_observations(spark):
    bronze = spark.table(SOURCE)
    required = {
        'record_id', 'object_key', 'record_type', 'source',
        'record_position', 'raw_record_json'
    }
    missing = required - set(bronze.columns)
    if missing:
        raise RuntimeError(
            'Fuel Bronze schema is missing required columns: '
            + ', '.join(sorted(missing))
        )

    # The three raw layouts are retained separately: comparing them is a Gold/DQ concern.
    df = bronze.select(
        F.col('record_id'), F.col('object_key'), F.col('source'), F.col('record_type'),
        F.col('record_position'),
        F.when(F.col('record_type') == 'bbw_json_data', F.lit('bbw_fuel'))
         .when(F.col('record_type') == 'legacy_json_list', F.lit('legacy_json_list'))
         .when(F.col('record_type') == 'legacy_csv', F.lit('legacy_csv'))
         .otherwise(F.lit('unsupported')).alias('source_family'),
        text_clean(F.coalesce(j('series_code'), j('series'), j('fuel_code'))).alias('series_code_raw'),
        text_clean(F.coalesce(j('region'), j('zone'), j('fuel_name'))).alias('region_raw'),
        F.coalesce(j('quote_date_vn'), j('quote_date'), j('date')).alias('date_raw'),
        F.coalesce(j('price'), j('price_vnd_liter'), j('price_vnd_kg')).alias('price_raw'),
        text_clean(F.coalesce(j('currency'), F.lit(''))).alias('currency_raw'),
        text_clean(F.coalesce(j('unit'), F.lit(''))).alias('unit_raw'),
        text_clean(j('product')).alias('product_raw'),
        j('quoted_at').alias('quoted_at_raw'),
        j('updated_at').alias('updated_at_raw'),
    )
    df = (df
        .withColumn('quote_date', F.to_date(F.substring('date_raw', 1, 10)))
        .withColumn('series_code', F.when(
            F.col('series_code_raw').isNotNull() & (F.col('series_code_raw') != ''),
            F.col('series_code_raw')
        ).otherwise(F.when(
            F.col('region_raw').isNotNull() & (F.col('region_raw') != ''),
            F.concat(F.lit('FUEL/'), F.col('region_raw'))
        )))
        .withColumn('region', F.coalesce('region_raw', F.lit('UNKNOWN')))
        .withColumn('unit', F.regexp_replace('unit_raw', ' ', ''))
        .withColumn('price', F.when(
            F.trim('price_raw').rlike(r'^\d+(?:\.\d+)?$'),
            F.trim('price_raw').cast('decimal(18,2)')
        ).otherwise(F.lit(None).cast('decimal(18,2)')))
        .withColumn('quoted_at', F.to_timestamp('quoted_at_raw'))
        .withColumn('updated_at', F.to_timestamp('updated_at_raw'))
    )
    valid = (
        (F.col('source_family') != 'unsupported')
        & F.col('record_id').isNotNull()
        & F.col('series_code').isNotNull()
        & F.col('quote_date').isNotNull()
        & F.col('price').isNotNull() & (F.col('price') > 0)
        & F.col('unit').isin('VND/L', 'VND/LITER', 'VND/LITRE', 'VND/KG')
        & F.col('currency_raw').isin('VND', '')
        & F.col('product_raw').isin('FUEL', '')
    )
    return df.withColumn('is_valid_fuel', F.coalesce(valid, F.lit(False)))


def make_canonical(valid):
    # Source family, region, date AND unit matter. Do not merge VND/kg with VND/liter.
    key = ['source_family', 'series_code', 'region', 'quote_date', 'unit']
    aggregated = valid.groupBy(*key).agg(
        F.count('*').alias('raw_observation_count'),
        F.countDistinct('price').alias('distinct_price_count'),
        F.min('price').alias('min_price'), F.max('price').alias('max_price'))
    ranking = Window.partitionBy(*key).orderBy(
        F.col('updated_at').desc_nulls_last(), F.col('quoted_at').desc_nulls_last(),
        F.col('object_key').asc(), F.col('record_position').asc(), F.col('record_id').asc())
    selected = (valid.withColumn('_rn', F.row_number().over(ranking))
                .filter(F.col('_rn') == 1).drop('_rn'))
    daily = selected.join(aggregated, key, 'inner').select(
        *key, F.col('price').alias('price_vnd_per_unit'),
        'raw_observation_count', 'distinct_price_count', 'min_price', 'max_price',
        F.col('quoted_at').alias('selected_quoted_at'),
        F.col('record_id').alias('selected_bronze_record_id'),
        F.col('object_key').alias('selected_bronze_object_key'))
    history = Window.partitionBy('source_family', 'series_code', 'region', 'unit').orderBy('quote_date')
    daily = daily.withColumn('_previous_price', F.lag('price_vnd_per_unit').over(history))
    daily = daily.withColumn('price_change_percent',
        F.when(F.col('_previous_price') > 0,
          F.round((F.col('price_vnd_per_unit') - F.col('_previous_price'))
                  / F.col('_previous_price') * 100, 3).cast('decimal(12,3)'))
         .otherwise(F.lit(None).cast('decimal(12,3)')))
    daily = daily.withColumn('price_jump_review', F.abs(F.col('price_change_percent')) > 25)
    daily = daily.withColumn('quality_status',
        F.when(F.col('distinct_price_count') > 1, 'PRICE_CONFLICT_REVIEW')
         .when(F.col('price_jump_review'), 'PRICE_JUMP_REVIEW')
         .otherwise('PASS'))
    return daily.drop('_previous_price').withColumn('silver_processed_at', F.current_timestamp())


def main():
    write = '--write' in sys.argv[1:]
    spark = (SparkSession.builder.appName('TLCN-Silver-Fuel')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.lakehouse.type', 'rest')
        .config('spark.sql.catalog.lakehouse.uri', 'http://nessie:19120/iceberg/main/')
        .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        src = build_observations(spark).cache()
        total = src.count()
        print('FUEL_SOURCE_TOTAL:', total, flush=True)
        if total == 0:
            raise RuntimeError('No source rows found in the Bronze fuel table')

        # Diagnose current ingestion volume; do not compare it to a stale fixed baseline.
        print('FUEL_SOURCE_BREAKDOWN:', flush=True)
        src.groupBy('source_family', 'record_type').count().orderBy(
            'source_family', 'record_type'
        ).show(100, truncate=False)

        unsupported = src.filter(F.col('source_family') == 'unsupported').count()
        print('FUEL_UNSUPPORTED_ROWS:', unsupported, flush=True)
        valid = src.filter(F.col('is_valid_fuel')).cache()
        accepted = valid.count()
        excluded = total - accepted
        print('FUEL_VALID_OBSERVATIONS:', accepted, flush=True)
        print('FUEL_EXCLUDED_OR_UNSUPPORTED:', excluded, flush=True)
        if accepted == 0:
            raise RuntimeError(
                'No valid fuel observations remain after validation; '
                'inspect FUEL_SOURCE_BREAKDOWN and Bronze raw records before writing'
            )

        canonical = make_canonical(valid).cache()
        rows = canonical.count()
        print('FUEL_CANONICAL_ROWS:', rows, flush=True)
        canonical.groupBy('source_family', 'quality_status').count().orderBy(
            'source_family', 'quality_status'
        ).show(50, truncate=False)
        print('FUEL_CONFLICT_EXAMPLES:', flush=True)
        canonical.filter('distinct_price_count > 1').select(
            'source_family', 'series_code', 'region', 'quote_date', 'unit',
            'raw_observation_count', 'min_price', 'max_price'
        ).show(20, truncate=False)
        print('FUEL_PRICE_JUMP_EXAMPLES:', flush=True)
        canonical.filter('price_jump_review').select(
            'source_family', 'series_code', 'region', 'quote_date', 'unit',
            'price_vnd_per_unit', 'price_change_percent', 'quality_status'
        ).show(20, truncate=False)

        key = ['source_family', 'series_code', 'region', 'quote_date', 'unit']
        distinct_keys = canonical.select(*key).distinct().count()
        if rows == 0 or rows > accepted or rows != distinct_keys:
            raise RuntimeError('Empty data or canonical business-key duplication')
        if canonical.filter(
            'selected_bronze_record_id IS NULL OR selected_bronze_object_key IS NULL'
        ).limit(1).count():
            raise RuntimeError('Missing Bronze lineage in canonical fuel observations')

        if not write:
            print('SILVER_FUEL_DRY_RUN_PASSED', flush=True)
            print('NO_SILVER_TABLE_WRITTEN', flush=True)
            return

        spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.silver')
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (
            source_family STRING, series_code STRING, region STRING, quote_date DATE, unit STRING,
            price_vnd_per_unit DECIMAL(18,2), raw_observation_count BIGINT,
            distinct_price_count BIGINT, min_price DECIMAL(18,2), max_price DECIMAL(18,2),
            selected_quoted_at TIMESTAMP, selected_bronze_record_id STRING,
            selected_bronze_object_key STRING, price_change_percent DECIMAL(12,3),
            price_jump_review BOOLEAN, quality_status STRING, silver_processed_at TIMESTAMP
        ) USING iceberg""")
        canonical.createOrReplaceTempView('fuel_silver_stage')
        spark.sql(f"""MERGE INTO {TARGET} t USING fuel_silver_stage s
            ON t.source_family=s.source_family AND t.series_code=s.series_code
            AND t.region=s.region AND t.quote_date=s.quote_date AND t.unit=s.unit
            WHEN MATCHED THEN UPDATE SET
                t.price_vnd_per_unit=s.price_vnd_per_unit,
                t.raw_observation_count=s.raw_observation_count,
                t.distinct_price_count=s.distinct_price_count,
                t.min_price=s.min_price, t.max_price=s.max_price,
                t.selected_quoted_at=s.selected_quoted_at,
                t.selected_bronze_record_id=s.selected_bronze_record_id,
                t.selected_bronze_object_key=s.selected_bronze_object_key,
                t.price_change_percent=s.price_change_percent,
                t.price_jump_review=s.price_jump_review,
                t.quality_status=s.quality_status
            WHEN NOT MATCHED THEN INSERT *""")
        current = spark.table(TARGET)
        n = current.count()
        target_distinct_keys = current.select(*key).distinct().count()
        print('SILVER_FUEL_TOTAL:', n, flush=True)
        if n != rows or n != target_distinct_keys:
            raise RuntimeError(
                f'Fuel Silver target has {n} rows and {target_distinct_keys} distinct keys; '
                f'current Bronze canonical stage has {rows} rows. '
                'Existing extra target rows were not deleted; inspect before cleanup.'
            )
        print('SILVER_FUEL_ICEBERG_PASSED', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
