"""TLCN / HUTECH: Bronze Iceberg VICOFA -> Silver Iceberg articles.

Default: read-only dry run. --write: CREATE / MERGE insert-only Silver table.
Classification is deterministic, multi-label Vietnamese keyword matching; NOT ML or sentiment.
List and detail records are joined by article id, with URL mismatch flagged.
Raw Bronze records are never changed. Keep original text and record-level lineage.
"""
import argparse
import re
import unicodedata
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

BRONZE = 'lakehouse.bronze.vicofa_articles_raw'
TARGET = 'lakehouse.silver.vicofa_articles'

# Rules are explicit and editable. Avoid generic 'coffee', 'Vietnam', 'agriculture' words.
KEYWORDS = {
    'PRICE_MARKET': ['gia ca phe', 'gia robusta', 'gia arabica', 'bien dong gia',
                     'thi truong ca phe', 'thi truong robusta', 'gia nong san',
                     'gia xuat khau', 'gia ky han', 'san london', 'gia the gioi', 'ton kho'],
    'EXPORT_TRADE': ['xuat khau', 'nhap khau', 'kim ngach', 'thuong mai',
                     'thi truong eu', 'thi truong my', 'thi truong trung quoc',
                     'hai quan', 'fta', 'doi tac thuong mai', 'logistics'],
    'PRODUCTION_SUPPLY': ['san luong', 'dien tich', 'nang suat', 'thu hoach',
                          'nguon cung', 'nguon nguyen lieu', 'mua vu', 'tai canh',
                          'vung trong', 'nong ho', 'nguoi trong ca phe', 'canh tac'],
    'WEATHER_CLIMATE': ['thoi tiet', 'khi hau', 'han han', 'mua lon', 'lu lut',
                        'el nino', 'la nina', 'nhiet do', 'luong mua', 'thien tai',
                        'han man', 'bien doi khi hau'],
    'POLICY_REGULATION': ['chinh sach', 'quy dinh', 'nghi dinh', 'thong tu',
                          'du thao luat', 'du an luat', 'thue quan', 'thue xuat khau',
                          'truy xuat nguon goc', 'eudr', 'quy che', 'bo nong nghiep',
                          'an toan thuc pham', 'phap ly'],
    'SUSTAINABILITY': ['ben vung', 'phat thai', 'carbon', 'phat trien xanh',
                       'kinh te tuan hoan', 'chung nhan', 'huu co',
                       'rung', 'bao ve moi truong', 'net zero'],
    'TECHNOLOGY_PROCESSING': ['che bien sau', 'che bien', 'cong nghe',
                              'chuyen doi so', 'doi moi sang tao', 'tu dong hoa',
                              'tri tue nhan tao', 'ung dung ai', 'cong nghe ai', 'khoa hoc ky thuat',
                              'rang xay', 'chat luong ca phe'],
    'ENTERPRISE_INVESTMENT': ['doanh nghiep', 'dau tu', 'nha may', 'von dau tu',
                              'hop tac', 'lien ket chuoi', 'thuong hieu',
                              'tap doan', 'kinh doanh', 'khoi nghiep'],
}


def normalize(text):
    text = unicodedata.normalize('NFD', (text or '').lower())
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn')
    return text.replace('đ', 'd')


# Boundary matching prevents 'ai' matching arbitrary syllables inside other words.
COMPILED = {
    category: [(kw, re.compile(r'(?<![a-z0-9])' + re.escape(kw) + r'(?![a-z0-9])'))
               for kw in words]
    for category, words in KEYWORDS.items()
}

SCHEMA = StructType([
    StructField('categories', ArrayType(StringType(), False), False),
    StructField('primary_category', StringType(), False),
    StructField('matched_keywords', ArrayType(StringType(), False), False),
])


# Common association notices are not market intelligence, even if their summary
# contains generic business vocabulary. Check headline only for this override.
NOTICE_PATTERNS = [
    re.compile(r'(?<![a-z0-9])thong bao.{0,35}(lich nghi|nghi le)(?![a-z0-9])'),
    re.compile(r'(?<![a-z0-9])lich nghi le(?![a-z0-9])'),
]


def classify(title, summary, content):
    """Deterministic labels: headline takes precedence over summary.

    Website boilerplate frequently leaks into article detail text, therefore
    content is intentionally NOT classified by default. It remains in Silver
    for search and later curated NLP/keyword processing.
    """
    headline = normalize(title)
    abstract = normalize(summary)
    if any(pattern.search(headline) for pattern in NOTICE_PATTERNS):
        return (['OTHER'], 'OTHER', [])

    scores, matched = {}, {}
    for category, rules in COMPILED.items():
        title_matches = [kw for kw, pattern in rules if pattern.search(headline)]
        summary_matches = [kw for kw, pattern in rules if pattern.search(abstract)]
        if not title_matches and not summary_matches:
            continue
        # One relevant keyword in the headline is stronger evidence than any
        # number of summary keywords for selecting the PRIMARY category.
        scores[category] = (int(bool(title_matches)), len(title_matches),
                            len(summary_matches))
        matched[category] = list(dict.fromkeys(title_matches + summary_matches))

    ordered = sorted(scores, key=lambda cat: (
        -scores[cat][0], -scores[cat][1], -scores[cat][2],
        list(KEYWORDS).index(cat)
    ))
    primary = ordered[0] if ordered else 'OTHER'
    annotations = [f'{cat}:{kw}' for cat in ordered for kw in matched[cat]]
    return (ordered if ordered else ['OTHER'], primary, annotations)


def spark_session():
    spark = (SparkSession.builder.appName('TLCN-Silver-VICOFA-Articles')
        .config('spark.sql.extensions',
                'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.lakehouse.type', 'rest')
        .config('spark.sql.catalog.lakehouse.uri', 'http://nessie:19120/iceberg/main/')
        .config('spark.sql.session.timeZone', 'Asia/Ho_Chi_Minh')
        .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def field(json_col, name):
    value = F.trim(json_col.getItem(name))
    return F.when(F.length(value) > 0, value)


def parse_date(value):
    return F.coalesce(F.to_date(value, 'dd-MM-yyyy'),
                      F.to_date(value, 'dd/MM/yyyy'),
                      F.to_date(value, 'yyyy-MM-dd'))


def source_rows(bronze, record_type, prefix):
    df = bronze.filter(F.col('record_type') == record_type)
    j = F.from_json('raw_record_json', 'MAP<STRING,STRING>')
    return df.select(
        field(j, 'id').alias('article_id'),
        field(j, 'url').alias(f'{prefix}_url'),
        field(j, 'title').alias(f'{prefix}_title'),
        field(j, 'title_detail').alias(f'{prefix}_title_detail'),
        field(j, 'date').alias(f'{prefix}_date'),
        field(j, 'date_detail').alias(f'{prefix}_date_detail'),
        field(j, 'summary').alias(f'{prefix}_summary'),
        field(j, 'content').alias(f'{prefix}_content'),
        F.col('record_id').alias(f'{prefix}_bronze_record_id'),
        F.col('object_key').alias(f'{prefix}_bronze_object_key'),
        F.col('record_position').alias(f'{prefix}_bronze_record_position'),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--write', action='store_true',
                        help='Write insert-only MERGE into Silver Iceberg')
    args = parser.parse_args()
    spark = spark_session()
    try:
        raw = spark.table(BRONZE)
        total = raw.count()
        print('VICOFA_BRONZE_TOTAL:', total, flush=True)
        raw.groupBy('record_type').count().show(truncate=False)
        if total != 2436:
            raise RuntimeError(f'Unexpected Bronze VICOFA rows: {total}; no writes')
        listing = source_rows(raw, 'vicofa_article_list', 'list').cache()
        detail = source_rows(raw, 'vicofa_article_detail', 'detail').cache()
        for name, data in [('LIST', listing), ('DETAIL', detail)]:
            count = data.count()
            unique = data.select('article_id').distinct().count()
            invalid = data.filter(F.col('article_id').isNull()).count()
            print(f'VICOFA_{name}_ROWS: {count}', flush=True)
            print(f'VICOFA_{name}_UNIQUE_IDS: {unique}', flush=True)
            if count != 1218 or unique != count or invalid:
                raise RuntimeError(f'{name}: unexpected/missing/duplicate ids; no writes')
        joined = detail.join(listing, 'article_id', 'full_outer')
        missing_pairs = joined.filter(
            F.col('detail_bronze_record_id').isNull() |
            F.col('list_bronze_record_id').isNull()
        ).count()
        print('VICOFA_UNPAIRED_IDS:', missing_pairs, flush=True)
        if missing_pairs:
            raise RuntimeError('Some article ids are missing list/detail pair; no writes')
        url_mismatch = (
            F.col('detail_url').isNotNull() & F.col('list_url').isNotNull() &
            (F.col('detail_url') != F.col('list_url'))
        )
        combined = joined.select(
            'article_id',
            F.coalesce('detail_url', 'list_url').alias('url'),
            F.coalesce('detail_title_detail', 'detail_title', 'list_title').alias('title'),
            F.coalesce('detail_summary', 'list_summary').alias('summary'),
            F.coalesce('detail_content', 'list_content').alias('_content_raw'),
            F.coalesce('detail_date_detail', 'detail_date', 'list_date').alias('_date_text'),
            url_mismatch.alias('url_mismatch_review'),
            'detail_bronze_record_id', 'detail_bronze_object_key',
            'detail_bronze_record_position',
            'list_bronze_record_id', 'list_bronze_object_key',
            'list_bronze_record_position',
        )
        # Minimal, reversible cleanup: remove HTML tags and collapse whitespace;
        # DO NOT silently remove alleged boilerplate or rewrite source content.
        text = F.regexp_replace(F.col('_content_raw'), r'(?is)<[^>]*>', ' ')
        text = F.regexp_replace(text, r'\s+', ' ')
        combined = (combined
            .withColumn('content_text', F.when(F.length(F.trim(text)) > 0, F.trim(text)))
            .withColumn('published_date', parse_date(F.col('_date_text')))
            .withColumn('content_missing', F.col('content_text').isNull())
        )
        # Classify from headline and summary only to avoid detail-page boilerplate.
        classify_udf = F.udf(classify, SCHEMA)
        combined = combined.withColumn(
            '_labels', classify_udf('title', 'summary', 'content_text')
        )
        result = combined.select(
            'article_id', 'url', 'title', 'published_date', 'summary', 'content_text',
            F.col('_labels.categories').alias('keyword_categories'),
            F.col('_labels.primary_category').alias('primary_category'),
            F.col('_labels.matched_keywords').alias('matched_keywords'),
            'content_missing', 'url_mismatch_review',
            F.when(F.col('url_mismatch_review'), 'URL_MISMATCH_REVIEW')
                .when(F.col('published_date').isNull(), 'DATE_REVIEW')
                .when(F.col('content_missing'), 'CONTENT_MISSING_REVIEW')
                .otherwise('PASS').alias('quality_status'),
            F.col('detail_bronze_record_id').alias('selected_bronze_record_id'),
            F.col('detail_bronze_object_key').alias('selected_bronze_object_key'),
            F.col('detail_bronze_record_position').alias('selected_bronze_record_position'),
            'list_bronze_record_id', 'list_bronze_object_key',
            'list_bronze_record_position',
            F.current_timestamp().alias('silver_processed_at'),
        ).cache()
        rows = result.count()
        unique = result.select('article_id').distinct().count()
        print('VICOFA_CANONICAL_ROWS:', rows, flush=True)
        print('VICOFA_UNIQUE_IDS:', unique, flush=True)
        print('VICOFA_CONTENT_MISSING:', result.filter('content_missing').count(), flush=True)
        print('VICOFA_URL_MISMATCH:', result.filter('url_mismatch_review').count(), flush=True)
        print('VICOFA_DATE_MISSING:', result.filter('published_date IS NULL').count(), flush=True)
        print('VICOFA_PRIMARY_CATEGORY_COUNTS:', flush=True)
        result.groupBy('primary_category').count().orderBy(F.desc('count')).show(30, False)
        print('VICOFA_QUALITY_STATUS:', flush=True)
        result.groupBy('quality_status').count().show(truncate=False)
        print('VICOFA_CATEGORY_EXAMPLES:', flush=True)
        result.select('article_id', 'title', 'primary_category',
                      'keyword_categories', 'matched_keywords').orderBy('article_id').show(12, truncate=90)
        if rows != 1218 or unique != rows:
            raise RuntimeError(f'Expected 1218 unique article ids, got {rows}/{unique}')
        critical = result.filter(F.col('url').isNull() | F.col('title').isNull() |
                                 F.col('published_date').isNull() | F.col('url_mismatch_review')).count()
        if critical:
            raise RuntimeError(f'{critical} articles with missing key fields or mismatched URLs; review before write')
        print('SILVER_VICOFA_DRY_RUN_PASSED', flush=True)
        if args.write:
            spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.silver')
            result.createOrReplaceTempView('vicofa_articles_stage')
            spark.sql(f'CREATE TABLE IF NOT EXISTS {TARGET} USING iceberg AS '
                      'SELECT * FROM vicofa_articles_stage WHERE 1=0')
            spark.sql(f'''MERGE INTO {TARGET} t USING vicofa_articles_stage s
              ON t.article_id = s.article_id
              WHEN NOT MATCHED THEN INSERT *''')
            stored = spark.table(TARGET)
            stored_total = stored.count()
            stored_unique = stored.select('article_id').distinct().count()
            print('SILVER_VICOFA_TOTAL:', stored_total, flush=True)
            if stored_total != 1218 or stored_unique != stored_total:
                raise RuntimeError(f'VICOFA Iceberg verification failed {stored_total}/{stored_unique}')
            print('SILVER_VICOFA_ICEBERG_PASSED', flush=True)
        else:
            print('NO_SILVER_TABLE_WRITTEN', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
