"""Read-only content profiling of every data object under MinIO bronze/.
Run from D:\\TLCN: python -m src.bronze.profile_minio_bronze --output-dir output/bronze_audit/content_profile
Requires: src.bronze.minio_client and installed minio SDK.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from src.bronze.minio_client import get_bucket_name, get_minio_client


def classify(key):
    parts = key.split('/')
    domain = parts[1] if len(parts) > 1 else 'unknown'
    source = next((part[7:] for part in parts if part.startswith('source=')), 'legacy_unknown')
    return domain, source


def read_object(client, bucket, key):
    response = client.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def flatten_json(value, prefix='', out=None):
    if out is None:
        out = {}
    if isinstance(value, dict):
        if not value:
            out[prefix or '$'] = '{}'
        for k, v in value.items():
            name = f'{prefix}.{k}' if prefix else str(k)
            if isinstance(v, dict):
                flatten_json(v, name, out)
            elif isinstance(v, list):
                out[name] = v
            else:
                out[name] = v
    else:
        out[prefix or 'value'] = value
    return out


def structured_rows(payload):
    """Select a useful set of records without silently discarding the original file."""
    if isinstance(payload, list):
        return payload, 'root_list'
    if not isinstance(payload, dict):
        return [payload], 'scalar'
    for name in ('data', 'results', 'records', 'items', 'observations'):
        value = payload.get(name)
        if isinstance(value, list):
            return value, name
    daily = payload.get('daily')
    if isinstance(daily, dict) and isinstance(daily.get('time'), list):
        fields = {k: v for k, v in daily.items() if isinstance(v, list)}
        n = len(daily['time'])
        return [{k: (vals[i] if i < len(vals) else None) for k, vals in fields.items()} for i in range(n)], 'daily_arrays'
    return [payload], 'root_object'


def parse_csv(content):
    text = content.decode('utf-8-sig', errors='replace')
    sample = text[:16384]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ','
    reader = csv.DictReader(io.StringIO(text, newline=''), delimiter=delimiter)
    if not reader.fieldnames:
        return [], 'csv_missing_header'
    return list(reader), f'csv_delimiter={delimiter!r}'


def parse_jsonl(content):
    text = content.decode('utf-8-sig', errors='replace')
    rows = []
    for idx, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f'Invalid JSONL at line {idx}: {exc}') from exc
    return rows, 'jsonl'


def parse_data(key, content):
    suffix = Path(key).suffix.lower()
    if suffix == '.csv':
        return parse_csv(content)
    if suffix in ('.jsonl', '.ndjson'):
        return parse_jsonl(content)
    if suffix == '.json':
        return structured_rows(json.loads(content.decode('utf-8-sig')))
    return [], 'unsupported_extension'


def is_missing(value):
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in ('', 'null', 'none', 'nan', 'na', 'n/a'):
        return True
    return False


def profile_rows(rows):
    columns = defaultdict(lambda: {'present': 0, 'null': 0, 'type_counts': Counter(), 'samples': []})
    total = len(rows)
    for raw in rows:
        obj = flatten_json(raw)
        for field, value in obj.items():
            col = columns[field]
            col['present'] += 1
            if is_missing(value):
                col['null'] += 1
            col['type_counts'][type(value).__name__] += 1
            if not is_missing(value) and len(col['samples']) < 3:
                sample = str(value)
                if sample not in col['samples']:
                    col['samples'].append(sample[:120])
    out = []
    for field, col in sorted(columns.items()):
        absent = total - col['present']
        out.append({'column': field, 'rows': total, 'missing_count': absent + col['null'],
                    'missing_percent': round(100 * (absent + col['null']) / total, 2) if total else 0,
                    'types': '; '.join(f'{k}:{v}' for k, v in sorted(col['type_counts'].items())),
                    'samples': ' | '.join(col['samples'])})
    return out


def save_csv(path, fields, rows):
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description='READ-ONLY profile of all MinIO Bronze CSV/JSON/JSONL objects')
    parser.add_argument('--prefix', default='bronze/')
    parser.add_argument('--output-dir', default='output/bronze_audit/content_profile')
    args = parser.parse_args()
    if args.prefix != 'bronze/':
        parser.error('This audit intentionally requires --prefix bronze/')
    client = get_minio_client()
    bucket = get_bucket_name()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f'Bucket not found: {bucket}')
    root = Path(args.output_dir)
    root.mkdir(parents=True, exist_ok=True)
    data_reports, field_reports, problems = [], [], []
    metadata_count = 0
    data_objects = []
    for obj in client.list_objects(bucket, prefix='bronze/', recursive=True):
        if obj.is_dir:
            continue
        key = obj.object_name
        if key.endswith('/metadata.json'):
            metadata_count += 1
            continue
        data_objects.append((key, obj))
    duplicates = defaultdict(list)
    for idx, (key, obj) in enumerate(sorted(data_objects), start=1):
        domain, source = classify(key)
        result = {'domain': domain, 'source': source, 'object_key': key,
                  'size_bytes': obj.size, 'format': Path(key).suffix.lower(),
                  'status': 'OK', 'row_count': '', 'field_count': '', 'structure': '',
                  'sha256': '', 'error': ''}
        try:
            content = read_object(client, bucket, key)
            result['sha256'] = hashlib.sha256(content).hexdigest()
            duplicates[result['sha256']].append(key)
            rows, structure = parse_data(key, content)
            result['structure'] = structure
            if structure == 'unsupported_extension':
                result['status'] = 'UNSUPPORTED'
                problems.append({'type': 'UNSUPPORTED_FORMAT', 'object_key': key, 'detail': result['format']})
            else:
                result['row_count'] = len(rows)
                cols = profile_rows(rows)
                result['field_count'] = len(cols)
                for col in cols:
                    field_reports.append({'domain': domain, 'source': source, 'object_key': key, **col})
                if not rows:
                    problems.append({'type': 'EMPTY_DATASET', 'object_key': key, 'detail': structure})
        except Exception as exc:
            result['status'] = 'ERROR'
            result['error'] = f'{type(exc).__name__}: {exc}'[:500]
            problems.append({'type': 'PARSE_OR_READ_ERROR', 'object_key': key, 'detail': result['error']})
        data_reports.append(result)
        print(f'[{idx}/{len(data_objects)}] {result["status"]}: {key} | rows={result["row_count"]}', flush=True)
    for sha, keys in duplicates.items():
        if len(keys) > 1:
            for key in keys[1:]:
                problems.append({'type': 'DUPLICATE_BYTES', 'object_key': key, 'detail': f'sha256={sha}; first={keys[0]}'})
    save_csv(root / 'object_profile.csv', list(data_reports[0]) if data_reports else
             ['domain','source','object_key','size_bytes','format','status','row_count','field_count','structure','sha256','error'], data_reports)
    save_csv(root / 'field_profile.csv', ['domain','source','object_key','column','rows','missing_count','missing_percent','types','samples'], field_reports)
    save_csv(root / 'findings.csv', ['type','object_key','detail'], problems)
    group = defaultdict(lambda: {'objects': 0, 'ok_objects': 0, 'rows': 0, 'parse_errors': 0})
    for row in data_reports:
        grp = group[(row['domain'], row['source'])]
        grp['objects'] += 1
        grp['ok_objects'] += row['status'] == 'OK'
        grp['parse_errors'] += row['status'] == 'ERROR'
        if isinstance(row['row_count'], int):
            grp['rows'] += row['row_count']
    source_rows = [{'domain': d, 'source': s, **counts} for (d,s),counts in sorted(group.items())]
    save_csv(root / 'source_summary.csv', ['domain','source','objects','ok_objects','rows','parse_errors'], source_rows)
    summary = {'audited_at_utc': datetime.now(timezone.utc).isoformat(), 'bucket': bucket,
               'prefix': 'bronze/', 'metadata_objects_excluded_from_content_profile': metadata_count,
               'data_objects_found': len(data_objects), 'data_objects_ok': sum(x['status']=='OK' for x in data_reports),
               'data_objects_error': sum(x['status']=='ERROR' for x in data_reports),
               'data_objects_unsupported': sum(x['status']=='UNSUPPORTED' for x in data_reports),
               'total_extracted_rows_before_deduplication': sum(x['row_count'] for x in data_reports if isinstance(x['row_count'], int)),
               'finding_counts': dict(Counter(x['type'] for x in problems)),
               'notes': ['READ-ONLY: no MinIO writes','Row counts use root list, common list fields, or Open-Meteo daily arrays',
                         'JSON other than those patterns is profiled as one root object; nested records might require source-specific checks',
                         'Field profile is descriptive; it is not business-rule validation',
                         'VICOFA local JSONL is not included until uploaded to MinIO']}
    (root / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f'REPORTS: {root.resolve()}')
    print('READ-ONLY: No MinIO object was changed.')

if __name__ == '__main__':
    main()
