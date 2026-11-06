"""Read-only deep checks on existing TLCN Bronze Iceberg tables.

Run from D:\\TLCN:
    .\\run_spark.ps1 -Script "src/iceberg/validate_bronze_deep_quality.py"

No MinIO/S3 writes, no Iceberg DDL/DML, no export staging. Report files only:
  output/bronze_audit/data_quality/deep_checks/

Metrics are diagnostics, NOT data deletion/cleaning criteria.
"""
from __future__ import annotations

import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession

OUT = Path('/workspace/output/bronze_audit/data_quality/deep_checks')
MAX_EXAMPLES = 200
# Conservative review flags, not a claim that values are incorrect.
PRICE_JUMP_THRESHOLD = 0.25
MAX_DAYS_BETWEEN_PRICE_OBSERVATIONS = 7
CROSS_SOURCE_DIFFERENCE_THRESHOLD = 0.15

TABLES = {
    'coffee_prices': 'lakehouse.bronze.coffee_prices_raw',
    'domestic_price': 'lakehouse.bronze.domestic_prices_raw',
    'fuel': 'lakehouse.bronze.fuel_prices_raw',
    'weather': 'lakehouse.bronze.weather_raw',
    'london_futures': 'lakehouse.bronze.london_futures_raw',
}


def parse_date(value):
    if not value:
        return None
    s = str(value).strip().split('T')[0].split(' ')[0]
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d', '%d.%m.%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def number(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        x = float(value)
        return x if math.isfinite(x) else None
    s = str(value).strip().replace(' ', '').replace('\u00a0', '')
    if not s or s in ('-', '--', '..', 'N/A'):
        return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.') if s.rfind(',') > s.rfind('.') else s.replace(',', '')
    elif ',' in s:
        s = s.replace(',', '') if len(s.split(',')[-1]) == 3 and s.count(',') <= 2 else s.replace(',', '.')
    try:
        x = float(s)
        return x if math.isfinite(x) else None
    except ValueError:
        return None


def norm_region(v):
    s = str(v or '').strip().upper()
    table = {
        'ĐẮK LẮK': 'DAK_LAK', 'DAK LAK': 'DAK_LAK', 'ĐAK LAK': 'DAK_LAK',
        'ĐẮK NÔNG': 'DAK_NONG', 'DAK NONG': 'DAK_NONG',
        'GIA LAI': 'GIA_LAI', 'LÂM ĐỒNG': 'LAM_DONG', 'LAM DONG': 'LAM_DONG',
        'VIỆT NAM': 'VN_AVG', 'VIET NAM': 'VN_AVG',
    }
    return table.get(s, s.replace('-', '_').replace(' ', '_'))


def csv_write(path, fields, rows):
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fields)
        w.writeheader()
        w.writerows(rows)


def valid_coffee_record(d, source_group):
    # Reject known non-coffee series (FX is present inside BBW COFFEE responses).
    region = norm_region(d.get('region') if source_group != 'enriched_csv' else d.get('province'))
    if not region or region.startswith('TY_GIA') or region.startswith('USD'):
        return None
    series = str(d.get('series_code') or '').upper()
    if any(x in series for x in ('TY_GIA', 'USD_VND', 'EXCHANGE', 'FX')):
        return None
    if source_group == 'enriched_csv':
        day = parse_date(d.get('date'))
        price = number(d.get('coffee_price_vnd_kg'))
        unit = 'VND/KG'
    else:
        day = parse_date(d.get('quote_date_vn'))
        price = number(d.get('price'))
        unit = str(d.get('unit') or '').upper().replace(' ', '')
        if unit in ('VND/TON', 'VND/TONNE'):
            price = price / 1000 if price is not None else None
        elif unit not in ('VND/KG', 'VND/KILOGRAM'):
            return None  # unknown unit: never compare as though it were VND/kg
    if not day or price is None or price <= 0:
        return None
    # Only four known provincial coffee regions can be safely aligned across sources.
    if region not in ('DAK_LAK', 'DAK_NONG', 'GIA_LAI', 'LAM_DONG'):
        return None
    return day, region, price


def group_coverage(dates_by_group, *, calendar, findings):
    rows = []
    for key, values in sorted(dates_by_group.items()):
        dates = sorted(values)
        if not dates:
            continue
        start, end = dates[0], dates[-1]
        if calendar == 'daily':
            expected = (end - start).days + 1
            missing = expected - len(dates)
        elif calendar == 'weekday':
            expected = sum(1 for i in range((end - start).days + 1)
                           if (start + timedelta(days=i)).weekday() < 5)
            present = sum(x.weekday() < 5 for x in dates)
            missing = max(0, expected - present)
        else:
            expected, missing = None, None
        gaps = [(b-a).days for a, b in zip(dates, dates[1:])]
        max_gap = max(gaps, default=0)
        rows.append(dict(series=key, calendar_assumption=calendar, first_date=start.isoformat(),
                         last_date=end.isoformat(), distinct_days=len(dates),
                         expected_days='' if expected is None else expected,
                         missing_days='' if missing is None else missing,
                         maximum_gap_days=max_gap))
        if missing and len(findings) < MAX_EXAMPLES:
            sample_missing = []
            span = (end-start).days
            for i in range(span+1):
                dt = start + timedelta(days=i)
                if dt not in values and (calendar == 'daily' or dt.weekday() < 5):
                    sample_missing.append(dt.isoformat())
                    if len(sample_missing) == 5:
                        break
            findings.append(dict(category='TEMPORAL_GAP', series=key, date='',
                                 metric=str(missing), detail='Missing sample: '+', '.join(sample_missing)))
    return rows


def main():
    spark = (SparkSession.builder.appName('TLCN-Bronze-Deep-Quality-ReadOnly')
             .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
             .config('spark.sql.catalog.lakehouse.type', 'rest')
             .config('spark.sql.catalog.lakehouse.uri', 'http://nessie:19120/iceberg/main/')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    OUT.mkdir(parents=True, exist_ok=True)
    counters = Counter()
    weather_dates = defaultdict(set)
    coffee_dates = defaultdict(set)
    futures_dates = defaultdict(set)
    fuel_dates = defaultdict(set)
    # Keep one price per day/region/record group for diagnostics; duplicates separately counted.
    price_groups = defaultdict(lambda: defaultdict(list))
    fuel_groups = defaultdict(lambda: defaultdict(list))
    findings = []
    try:
        for domain, table in TABLES.items():
            for rec in spark.table(table).toLocalIterator():
                row = rec.asDict(recursive=True)
                counters[f'{domain}_rows'] += 1
                try:
                    d = json.loads(row['raw_record_json'])
                except (TypeError, ValueError):
                    counters[f'{domain}_invalid_json'] += 1
                    continue
                if not isinstance(d, dict):
                    counters[f'{domain}_nonobject_json'] += 1
                    continue
                if domain in ('coffee_prices', 'domestic_price'):
                    group = ('bbw_coffee' if domain == 'coffee_prices' else
                             'enriched_csv' if row.get('record_type') == 'enriched_csv' else 'legacy_api_json')
                    parsed = valid_coffee_record(d, group)
                    if not parsed:
                        counters[f'{group}_not_comparable_coffee'] += 1
                        continue
                    day, region, price = parsed
                    coffee_dates[f'{group}/{region}'].add(day)
                    price_groups[(group, region)][day].append(price)
                    counters[f'{group}_valid_comparable'] += 1
                elif domain == 'weather':
                    day = parse_date(d.get('time'))
                    if not day:
                        counters['weather_invalid_date'] += 1
                        continue
                    source = str(row.get('source') or 'unknown')
                    context = {}
                    if row.get('source_context_json'):
                        try:
                            context = json.loads(row['source_context_json'])
                        except (TypeError, ValueError):
                            counters['weather_invalid_context'] += 1
                    if not isinstance(context, dict):
                        context = {}
                    lat = context.get('latitude', context.get('grid_latitude', 'unknown'))
                    lon = context.get('longitude', context.get('grid_longitude', 'unknown'))
                    station = f'{source}/lat={lat}/lon={lon}'
                    weather_dates[station].add(day)
                elif domain == 'fuel':
                    day = parse_date(d.get('quote_date_vn') or d.get('quote_date'))
                    if not day:
                        counters['fuel_invalid_date'] += 1
                        continue
                    grp = str(row.get('source') or row.get('domain') or 'legacy')
                    product = str(d.get('series_code') or d.get('fuel_name') or d.get('product') or 'unknown')
                    zone = str(d.get('region') or d.get('zone') or 'unknown')
                    key = f'{grp}/{product}/{zone}'
                    fuel_dates[key].add(day)
                    unit = str(d.get('unit') or '').upper().replace(' ', '')
                    price = number(d.get('price') if d.get('price') is not None else d.get('price_vnd_liter'))
                    if price is not None and price > 0 and (not unit or unit in ('VND/L', 'VND/LITER', 'VND/LITRE')):
                        fuel_groups[key][day].append(price)
                elif domain == 'london_futures':
                    day = parse_date(d.get('Ngày'))
                    if day:
                        futures_dates['london_futures/all_raw_sources'].add(day)
                    else:
                        counters['futures_invalid_date'] += 1
            print(f'DEEP_SOURCE: {domain} rows={counters[f"{domain}_rows"]}', flush=True)

        coverage = []
        coverage += group_coverage(weather_dates, calendar='daily', findings=findings)
        # Coffee prices do NOT necessarily publish every calendar day; report observation gaps only.
        coverage += group_coverage(coffee_dates, calendar='event', findings=findings)
        coverage += group_coverage(fuel_dates, calendar='event', findings=findings)
        # Weekday gaps are indicators; exchange holidays remain legitimate.
        coverage += group_coverage(futures_dates, calendar='weekday', findings=findings)

        # Consolidate duplicate snapshots per source/region/date into median for diagnostics only.
        daily_medians = {}
        for (source, region), by_date in price_groups.items():
            daily_medians[(source, region)] = {day: statistics.median(vals) for day, vals in by_date.items()}
            counters['coffee_same_day_duplicate_observations'] += sum(max(0, len(vals)-1) for vals in by_date.values())
        comparisons = []
        if ('bbw_coffee', 'DAK_LAK') not in daily_medians:
            counters['bbw_dak_lak_unavailable_for_comparison'] = 1
        # Compare only matching date + region and VND/kg; no forced fills or unit guesses.
        for other in ('legacy_api_json', 'enriched_csv'):
            for region in ('DAK_LAK', 'DAK_NONG', 'GIA_LAI', 'LAM_DONG'):
                base = daily_medians.get(('bbw_coffee', region), {})
                alt = daily_medians.get((other, region), {})
                matched_days = sorted(set(base) & set(alt))
                diffs = [abs(base[d] - alt[d]) / max(base[d], alt[d]) for d in matched_days]
                big = sum(x >= CROSS_SOURCE_DIFFERENCE_THRESHOLD for x in diffs)
                comparisons.append(dict(reference='bbw_coffee', compared_with=other, region=region,
                                        matched_days=len(matched_days),
                                        median_relative_difference_pct=round(100*statistics.median(diffs), 3) if diffs else '',
                                        max_relative_difference_pct=round(100*max(diffs), 3) if diffs else '',
                                        days_ge_15pct_difference=big,
                                        status='NO_OVERLAP' if not diffs else 'REVIEW' if big else 'SCREEN_OK'))
                counters['cross_source_matched_days'] += len(matched_days)
                counters['cross_source_large_diff_days'] += big
                if big and len(findings) < MAX_EXAMPLES:
                    for day in matched_days:
                        diff = abs(base[day] - alt[day]) / max(base[day], alt[day])
                        if diff >= CROSS_SOURCE_DIFFERENCE_THRESHOLD:
                            findings.append(dict(category='CROSS_SOURCE_PRICE_DIFFERENCE',
                                                 series=f'bbw_coffee vs {other}/{region}', date=day.isoformat(),
                                                 metric=round(100*diff, 3), detail=f'bbw={base[day]}; other={alt[day]}; unit=VND/kg'))
                            if len(findings) >= MAX_EXAMPLES:
                                break

        outliers = []
        for (source, region), by_date in sorted(daily_medians.items()):
            ordered = sorted(by_date.items())
            for (day_a, a), (day_b, b) in zip(ordered, ordered[1:]):
                interval = (day_b-day_a).days
                if a <= 0 or interval > MAX_DAYS_BETWEEN_PRICE_OBSERVATIONS:
                    continue
                jump = (b-a)/a
                if abs(jump) >= PRICE_JUMP_THRESHOLD:
                    counters['coffee_price_jump_flags'] += 1
                    outliers.append(dict(domain='coffee', series=f'{source}/{region}', date=day_b.isoformat(),
                                         previous_date=day_a.isoformat(), previous_price=a, price=b,
                                         change_percent=round(100*jump, 3), gap_days=interval,
                                         review_flag='REVIEW_ONLY'))
        for series, by_date in sorted(fuel_groups.items()):
            ordered = sorted((d, statistics.median(vals)) for d, vals in by_date.items())
            for (day_a, a), (day_b, b) in zip(ordered, ordered[1:]):
                interval = (day_b-day_a).days
                if a <= 0 or interval > MAX_DAYS_BETWEEN_PRICE_OBSERVATIONS:
                    continue
                jump = (b-a)/a
                if abs(jump) >= PRICE_JUMP_THRESHOLD:
                    counters['fuel_price_jump_flags'] += 1
                    outliers.append(dict(domain='fuel', series=series, date=day_b.isoformat(),
                                         previous_date=day_a.isoformat(), previous_price=a, price=b,
                                         change_percent=round(100*jump, 3), gap_days=interval,
                                         review_flag='REVIEW_ONLY'))

        csv_write(OUT/'temporal_coverage.csv',
                  ['series','calendar_assumption','first_date','last_date','distinct_days','expected_days','missing_days','maximum_gap_days'], coverage)
        csv_write(OUT/'coffee_cross_source.csv',
                  ['reference','compared_with','region','matched_days','median_relative_difference_pct','max_relative_difference_pct','days_ge_15pct_difference','status'], comparisons)
        csv_write(OUT/'price_change_flags.csv',
                  ['domain','series','date','previous_date','previous_price','price','change_percent','gap_days','review_flag'], outliers)
        csv_write(OUT/'examples.csv', ['category','series','date','metric','detail'], findings)
        summary = {
            'evaluated_at_utc': datetime.now(timezone.utc).isoformat(),
            'read_only': True,
            'source': 'existing Bronze Iceberg tables populated from MinIO',
            'table_records': {name: counters[f'{name}_rows'] for name in TABLES},
            'coverage_series': len(coverage),
            'weather_series': len(weather_dates),
            'weather_series_with_missing_days': sum(r['calendar_assumption']=='daily' and isinstance(r['missing_days'], int) and r['missing_days']>0 for r in coverage),
            'coffee_cross_source_matched_days': counters['cross_source_matched_days'],
            'coffee_cross_source_large_difference_days': counters['cross_source_large_diff_days'],
            'price_change_flags': len(outliers),
            'counters': dict(sorted(counters.items())),
            'rules_and_limitations': [
                'Coverage is measured only between first and last observed date in each series; it does not prove completeness beyond that window.',
                'Weather group keys include source and source_context coordinates; coordinate versions must not be combined.',
                'Coffee and fuel use event observation gaps, not calendar-day missing counts.',
                'Futures weekday gaps include exchange holidays; do not label every gap as invalid.',
                'Cross-source only compares same date, recognized province, explicitly comparable VND/kg units.',
                'Multiple raw snapshots on one date are reduced to a median only for diagnostics; Bronze is unchanged.',
                '15% cross-source difference and 25% price jump are review thresholds, not anomaly proof or deletion criteria.',
                'No geographic station reference matching, no weather-value outlier test, no business-day market calendar.',
                'Do not use these outputs as automatic Silver deduplication rules without source-specific review.'
            ]
        }
        (OUT/'deep_quality_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2)+'\n',encoding='utf-8')
        print('DEEP_WEATHER_SERIES:', len(weather_dates), flush=True)
        print('DEEP_CROSS_SOURCE_MATCHED_DAYS:', counters['cross_source_matched_days'], flush=True)
        print('DEEP_PRICE_CHANGE_FLAGS:', len(outliers), flush=True)
        print('DEEP_REPORTS:', OUT, flush=True)
        print('BRONZE_DEEP_QUALITY_PASSED', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
