"""Read-only business quality assessment for TLCN Bronze Iceberg tables.

Run: .\run_spark.ps1 -Script 'src/iceberg/validate_bronze_quality.py'
Writes LOCAL reports to /workspace/output/bronze_audit/data_quality/.
Does not mutate MinIO, Nessie, Iceberg, or pre-existing profiling reports.

Rules are conservative screening checks, NOT scientific anomaly thresholds.
Duplication is assessed within a dataset/record type and is expected for
re-ingestion and byte-identical legacy copies; Silver can resolve it later.
"""
from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

OUT = Path('/workspace/output/bronze_audit/data_quality')
TABLES = {
    'coffee_prices': 'coffee_prices_raw',
    'domestic_price': 'domestic_prices_raw',
    'fuel': 'fuel_prices_raw',
    'weather': 'weather_raw',
    'london_futures': 'london_futures_raw',
    'area': 'coffee_area_raw',
    'production': 'coffee_production_raw',
    'trade': 'coffee_trade_raw',
    'vicofa': 'vicofa_articles_raw',
}
RULES = {
    'INVALID_RAW_JSON': ('FAIL', 'raw_record_json is not a JSON object'),
    'MISSING_RECORD_ID': ('FAIL', 'Missing record_id or object_key'),
    'MISSING_DATE': ('WARN', 'Missing primary source observation/publication date'),
    'INVALID_DATE': ('FAIL', 'Nonparseable date in known source formats'),
    'MISSING_PRICE': ('WARN', 'No price value for price observation'),
    'INVALID_PRICE': ('FAIL', 'Non-numeric or nonpositive price value'),
    'UNEXPECTED_UNIT': ('WARN', 'Price unit missing or not recognized; review before Silver normalization'),
    'MISSING_REGION': ('WARN', 'Missing locality/region in provincial coffee observation'),
    'MISSING_WEATHER_VALUE': ('WARN', 'Null in a recorded daily weather metric'),
    'INVALID_WEATHER_RANGE': ('FAIL', 'Physically invalid weather measurement'),
    'INVALID_OHLC': ('WARN', 'Futures OHLC values inconsistent or not interpretable'),
    'MISSING_FUTURES_VOLUME': ('WARN', 'Futures trading volume missing'),
    'INVALID_YEAR': ('FAIL', 'Historical statistical year invalid'),
    'INVALID_STAT_VALUE': ('FAIL', 'Historical area or production value non-numeric/negative'),
    'INVALID_TRADE_VALUE': ('WARN', 'Trade annual value non-numeric or negative'),
    'WORLD_AGGREGATE': ('INFO', 'World aggregate partner row; avoid double counting'),
    'VICOFA_NO_CONTENT': ('WARN', 'VICOFA detail article has missing/blank content'),
    'VICOFA_BOILERPLATE': ('FAIL', 'VICOFA content contains only repeated association contact information'),
    'VICOFA_MISSING_ID_URL': ('FAIL', 'VICOFA missing article ID or URL'),
    'VICOFA_DUPLICATE_ARTICLE_KEY': ('WARN', 'Repeated VICOFA article ID/URL within same record type'),
    'BUSINESS_KEY_REPEATED': ('WARN', 'Repeated source-level business key, including legitimate snapshot revisions'),
    'AREA_PRODUCTION_MISSING_YEAR': ('WARN', 'Missing year within observed historical range'),
    'OBJECT_HASH_DUPLICATED': ('INFO', 'Byte-identical raw source objects, retained intentionally'),
    'MINIO_PROFILE_ROW_DIFFERENCE': ('INFO', 'CSV weather station header rows excluded from Bronze Iceberg'),
}
MAX_SAMPLES_PER_GROUP = 12


def blank(v):
    return v is None or (isinstance(v, str) and not v.strip())


def numeric(v, *, vietnamese=False):
    if blank(v):
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if math.isfinite(float(v)) else None
    t = str(v).strip().replace(' ', '').replace('\u00a0', '').replace('%', '')
    t = t.replace('−', '-').replace('–', '-')
    # For London locale numbers: "2.345,6" -> 2345.6, "2,345.6" -> 2345.6
    if ',' in t and '.' in t:
        t = t.replace('.', '').replace(',', '.') if t.rfind(',') > t.rfind('.') else t.replace(',', '')
    elif ',' in t:
        t = t.replace(',', '.') if vietnamese else (t.replace(',', '') if re.fullmatch(r'-?\d{1,3}(,\d{3})+', t) else t.replace(',', '.'))
    try:
        n = float(t)
        return n if math.isfinite(n) else None
    except ValueError:
        return None


def valid_date(v):
    if blank(v):
        return False
    t = str(v).strip().split('T')[0].split(' ')[0]
    from datetime import datetime as dt
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d.%m.%Y'):
        try:
            dt.strptime(t, fmt)
            return True
        except ValueError:
            pass
    return False


def rule_engine():
    tally = defaultdict(lambda: {'evaluated': 0, 'violations': 0, 'examples': []})
    source_count = Counter()
    business_keys = defaultdict(set)
    object_hashes = defaultdict(set)
    historical_years = defaultdict(set)
    sample_findings = []

    def mark(domain, source, rule, record_id='', object_key='', detail='', violate=True):
        t = tally[(domain, source, rule)]
        t['evaluated'] += 1
        if violate:
            t['violations'] += 1
            if len(t['examples']) < MAX_SAMPLES_PER_GROUP:
                t['examples'].append({'record_id': str(record_id or ''), 'object_key': str(object_key or ''), 'detail': str(detail)[:400]})

    def parse_record(domain, source, row):
        rid = row.get('record_id')
        key = row.get('object_key')
        typ = row.get('record_type', '')
        raw = row.get('raw_record_json')
        if blank(rid) or blank(key):
            mark(domain, source, 'MISSING_RECORD_ID', rid, key)
        try:
            d = json.loads(raw) if isinstance(raw, str) else None
        except (ValueError, TypeError):
            d = None
        if not isinstance(d, dict):
            mark(domain, source, 'INVALID_RAW_JSON', rid, key)
            return
        digest = row.get('object_sha256')
        if digest:
            object_hashes[domain, digest].add(key)

        def issue(rule, bad, detail=''):
            mark(domain, source, rule, rid, key, detail, bool(bad))

        def date_check(value):
            issue('MISSING_DATE', blank(value))
            if not blank(value):
                issue('INVALID_DATE', not valid_date(value), str(value))

        def positive_price(value):
            issue('MISSING_PRICE', blank(value))
            if not blank(value):
                x = numeric(value)
                issue('INVALID_PRICE', x is None or x <= 0, str(value))

        def unique(key_parts):
            if any(blank(v) for v in key_parts):
                return
            k = (domain, typ, tuple(str(v).strip().casefold() for v in key_parts))
            repeated = k in business_keys[(domain, typ)]
            issue('BUSINESS_KEY_REPEATED', repeated, ' | '.join(str(v) for v in key_parts)[:300])
            business_keys[(domain, typ)].add(k)

        if domain in ('coffee_prices', 'domestic_price'):
            if typ == 'enriched_csv':
                day = d.get('date')
                value = d.get('coffee_price_vnd_kg')
                region = d.get('province')
                date_check(day)
                positive_price(value)
                issue('MISSING_REGION', blank(region))
                unique([day, region, d.get('commodity') or 'coffee'])
            else:
                day = d.get('quote_date_vn')
                value = d.get('price')
                region = d.get('region')
                date_check(day)
                positive_price(value)
                issue('MISSING_REGION', blank(region))
                unit = d.get('unit')
                issue('UNEXPECTED_UNIT', blank(unit) or str(unit).upper().replace(' ', '') not in ('VND/KG','VND/KILOGRAM','VND/TON','VND/TONNE','VND/TẤN'), str(unit))
                # Provider/series can have multiple updates and snapshot copies.
                unique([day, region, d.get('series_code'), (d.get('source') or {}).get('code') if isinstance(d.get('source'), dict) else d.get('source')])

        elif domain == 'fuel':
            day = d.get('quote_date_vn') or d.get('quote_date')
            value = d.get('price') if not blank(d.get('price')) else d.get('price_vnd_liter')
            date_check(day)
            positive_price(value)
            unit = d.get('unit')
            if not blank(unit):
                issue('UNEXPECTED_UNIT', str(unit).upper().replace(' ', '') not in ('VND/L','VND/LITER','VND/LITRE','VND/KG'), str(unit))
            unique([day, d.get('series_code') or d.get('fuel_name'), d.get('region') or d.get('zone')])

        elif domain == 'weather':
            day = d.get('time')
            date_check(day)
            for field, value in d.items():
                if field in ('time', 'weather_code'):
                    continue
                if value is None:
                    issue('MISSING_WEATHER_VALUE', True, field)
                    continue
                n = numeric(value)
                if field.startswith(('rain_', 'precipitation_', 'snowfall_', 'et0_')):
                    issue('INVALID_WEATHER_RANGE', n is None or n < 0, f'{field}={value}')
                elif field.startswith('temperature_'):
                    issue('INVALID_WEATHER_RANGE', n is None or n < -100 or n > 70, f'{field}={value}')
            # Deliberately no cross-version station dedup here: coordinates changed.

        elif domain == 'london_futures':
            date_check(d.get('Ngày'))
            vals = {x: numeric(d.get(x), vietnamese=True) for x in ('Lần cuối', 'Mở', 'Cao', 'Thấp')}
            if all(v is not None for v in vals.values()):
                high, low = vals['Cao'], vals['Thấp']
                bad = high < low or any(not low <= vals[n] <= high for n in ('Mở', 'Lần cuối'))
                issue('INVALID_OHLC', bad, str(vals))
            else:
                issue('INVALID_OHLC', True, str({k: d.get(k) for k in vals}))
            issue('MISSING_FUTURES_VOLUME', blank(d.get('KL')) or str(d.get('KL')).strip() in ('-', '--'))

        elif domain in ('area', 'production'):
            y = numeric(d.get('year'))
            bad_year = y is None or not y.is_integer() or y < 1900 or y > 2100
            issue('INVALID_YEAR', bad_year, str(d.get('year')))
            if not bad_year:
                historical_years[domain].add(int(y))
            field = 'coffee_area_thousand_ha' if domain == 'area' else 'coffee_production_thousand_tons'
            n = numeric(d.get(field))
            issue('INVALID_STAT_VALUE', n is None or n < 0, f'{field}={d.get(field)}')

        elif domain == 'trade':
            partner = str(d.get('partnerLabel') or '').strip().lower()
            issue('WORLD_AGGREGATE', partner in ('world', 'thế giới', 'all countries'), partner)
            for year in range(2001, 2025):
                field = f'{year} (USD Thousand)'
                value = d.get(field)
                if blank(value) or str(value).strip() in ('-', '..', '...', 'N/A'):
                    continue  # missing is profiled by field_profile; do not misreport as invalid
                n = numeric(value)
                issue('INVALID_TRADE_VALUE', n is None or n < 0, f'{field}={value}')

        elif domain == 'vicofa':
            article_key = d.get('id') or d.get('url')
            issue('VICOFA_MISSING_ID_URL', blank(d.get('id')) or blank(d.get('url')))
            date_check(d.get('date'))
            identity = (typ, str(article_key))
            repeated = identity in business_keys[('vicofa', 'article')]
            issue('VICOFA_DUPLICATE_ARTICLE_KEY', repeated, str(identity))
            business_keys[('vicofa', 'article')].add(identity)
            if typ == 'vicofa_article_detail':
                content = d.get('content')
                issue('VICOFA_NO_CONTENT', blank(content))
                boilerplate = isinstance(content, str) and ('HIỆP HỘI CÀ PHÊ - CA CAO VIỆT NAM (VICOFA)' in content.upper()) and ('NGƯỜI ĐẠI DIỆN:' in content.upper())
                issue('VICOFA_BOILERPLATE', boilerplate, str(d.get('id')))

    spark = (SparkSession.builder.appName('TLCN-Bronze-Business-Quality-ReadOnly')
             .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
             .config('spark.sql.catalog.lakehouse.type', 'rest')
             .config('spark.sql.catalog.lakehouse.uri', 'http://nessie:19120/iceberg/main/')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    table_totals = {}
    try:
        for domain, tab in TABLES.items():
            table = 'lakehouse.bronze.' + tab
            count = 0
            # Streaming driver iterator: no local staging, no Spark write.
            for rec in spark.table(table).toLocalIterator():
                row = rec.asDict(recursive=True)
                source = row.get('source') or row.get('source_layout') or 'unknown'
                if domain == 'domestic_price':
                    source = 'historical_enriched' if row.get('record_type') == 'enriched_csv' else 'legacy_api_json'
                if domain == 'vicofa':
                    source = row.get('record_type') or 'vicofa'
                source_count[(domain, source)] += 1
                parse_record(domain, source, row)
                count += 1
            table_totals[domain] = count
            print(f'QUALITY_SOURCE: {domain} records={count}', flush=True)

        for domain in ('area', 'production'):
            years = historical_years[domain]
            if years:
                missing = sorted(set(range(min(years), max(years) + 1)) - years)
                mark(domain, 'all_sources', 'AREA_PRODUCTION_MISSING_YEAR', detail=','.join(map(str, missing)), violate=bool(missing))

        for (domain, digest), objects in object_hashes.items():
            if len(objects) > 1:
                mark(domain, 'all_sources', 'OBJECT_HASH_DUPLICATED', detail=f'{len(objects)} objects with hash {digest[:12]}', violate=True)

        # Cross-check immutable snapshot counts against the previously saved MinIO profiling.
        profile_path = Path('/workspace/output/bronze_audit/content_profile/summary.json')
        baseline = None
        if profile_path.is_file():
            baseline = json.loads(profile_path.read_text(encoding='utf-8-sig'))
            # CSV metadata rows: 14 weather CSV objects * 2 = 28 lines.
            profile_rows = baseline.get('total_extracted_rows_before_deduplication')
            iceberg_rows = sum(table_totals.values())
            if isinstance(profile_rows, int) and profile_rows != iceberg_rows:
                mark('all', 'all_sources', 'MINIO_PROFILE_ROW_DIFFERENCE', detail=f'MinIO profile={profile_rows}; Iceberg={iceberg_rows}; gap={profile_rows - iceberg_rows}. Verify weather CSV header accounting.', violate=True)

        OUT.mkdir(parents=True, exist_ok=True)
        entries = []
        for (domain, source, rule), stats in sorted(tally.items()):
            severity, description = RULES[rule]
            status = 'PASS' if stats['violations'] == 0 else ('FAIL' if severity == 'FAIL' else 'WARN' if severity == 'WARN' else 'INFO')
            entries.append(dict(domain=domain, source=source, rule_id=rule, severity=severity,
                                status=status, evaluated=stats['evaluated'], violations=stats['violations'], description=description))
            for example in stats['examples']:
                sample_findings.append(dict(domain=domain, source=source, rule_id=rule, severity=severity, **example))

        def write_csv(filename, columns, records):
            with (OUT / filename).open('w', encoding='utf-8-sig', newline='') as fh:
                writer = csv.DictWriter(fh, fieldnames=columns)
                writer.writeheader()
                writer.writerows(records)

        write_csv('quality_rules.csv', ['domain','source','rule_id','severity','status','evaluated','violations','description'], entries)
        write_csv('quality_findings.csv', ['domain','source','rule_id','severity','record_id','object_key','detail'], sample_findings)
        by_source = []
        for (domain, source), count in sorted(source_count.items()):
            subset = [v for v in entries if v['domain'] == domain and v['source'] == source]
            by_source.append(dict(domain=domain, source=source, records=count,
                                  rules_evaluated=len(subset), rules_with_findings=sum(v['violations'] > 0 for v in subset),
                                  finding_instances=sum(v['violations'] for v in subset)))
        write_csv('quality_by_source.csv', ['domain','source','records','rules_evaluated','rules_with_findings','finding_instances'], by_source)
        summary = {
            'evaluated_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'Bronze Iceberg content derived from MinIO; comparison to saved MinIO profiling',
            'read_only': True,
            'table_records': table_totals,
            'total_rows_scanned': sum(table_totals.values()),
            'rules_with_findings': sum(e['violations'] > 0 for e in entries),
            'rule_violation_instances': sum(e['violations'] for e in entries),
            'sample_findings_written': len(sample_findings),
            'baseline_minio_profile': baseline.get('audited_at_utc') if isinstance(baseline, dict) else None,
            'limitations': [
                'Flags are screening results, not automatic deletion/normalization decisions.',
                'Duplicate business keys may represent valid revisions or repeated ingestion.',
                'Source-specific units and providers must be resolved before Silver.',
                'quality_findings.csv contains capped examples, not every violating record.',
                'No cross-source price reconciliation, weather-day completeness, or statistical outlier detection yet.',
                'ICEBERG profile does not re-download all 311 MinIO objects; earlier read-only MinIO audit/profile remains its source baseline.'
            ]
        }
        (OUT / 'quality_summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print('QUALITY_TOTAL_ROWS:', summary['total_rows_scanned'], flush=True)
        print('QUALITY_RULES_WITH_FINDINGS:', summary['rules_with_findings'], flush=True)
        print('QUALITY_REPORTS:', OUT, flush=True)
        print('BRONZE_BUSINESS_QUALITY_REPORT_PASSED', flush=True)
    finally:
        spark.stop()

if __name__ == '__main__':
    rule_engine()
