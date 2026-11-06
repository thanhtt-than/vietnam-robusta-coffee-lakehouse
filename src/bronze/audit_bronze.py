"""Read-only audit of Bronze objects in MinIO.

Run: python -m src.bronze.audit_bronze
Optional: --prefix bronze/ --output-dir reports/bronze_audit
"""
import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from src.bronze.minio_client import get_bucket_name, get_minio_client


def read_object(client, bucket, key):
    response = client.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def classification(key):
    parts = key.split('/')
    domain = parts[1] if len(parts) > 1 else 'unknown'
    source = next((p.split('=', 1)[1] for p in parts if p.startswith('source=')), 'legacy_unknown')
    return domain, source


def main():
    parser = argparse.ArgumentParser(description='Audit MinIO Bronze without modifying any object')
    parser.add_argument('--prefix', default='bronze/')
    parser.add_argument('--output-dir', default='reports/bronze_audit')
    args = parser.parse_args()
    if not args.prefix.startswith('bronze/'):
        parser.error('Only Bronze prefixes are allowed')

    client = get_minio_client()
    bucket = get_bucket_name()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f'Bucket does not exist: {bucket}')

    objects = {o.object_name: o for o in client.list_objects(bucket, prefix=args.prefix, recursive=True) if not o.is_dir}
    metadata_keys = sorted(k for k in objects if k.endswith('/metadata.json'))
    raw_keys = sorted(k for k in objects if '/raw/' in k)
    legacy_keys = sorted(k for k in objects if not any(p.startswith('source=') for p in k.split('/')))
    refs = defaultdict(list)
    findings = []
    metadata_valid = 0
    checked = 0
    matched = 0
    sha_to_keys = defaultdict(list)

    for meta_key in metadata_keys:
        domain, source = classification(meta_key)
        try:
            meta = json.loads(read_object(client, bucket, meta_key))
            if not isinstance(meta, dict):
                raise ValueError('metadata is not a JSON object')
            raw_key = meta.get('raw_object_key')
            if not isinstance(raw_key, str) or not raw_key:
                raise ValueError('raw_object_key is missing')
            refs[raw_key].append(meta_key)
            metadata_valid += 1
            if raw_key not in objects:
                findings.append(dict(type='MISSING_RAW', domain=domain, source=source, object_key=meta_key, detail=raw_key))
                continue
            raw = read_object(client, bucket, raw_key)
            checked += 1
            actual_sha = hashlib.sha256(raw).hexdigest()
            actual_size = len(raw)
            expected_sha = meta.get('sha256')
            expected_size = meta.get('size_bytes')
            if actual_sha == expected_sha and actual_size == expected_size:
                matched += 1
            else:
                findings.append(dict(type='INTEGRITY_MISMATCH', domain=domain, source=source, object_key=raw_key,
                                     detail=f'actual_size={actual_size}, expected_size={expected_size}, sha_match={actual_sha == expected_sha}'))
        except Exception as exc:
            findings.append(dict(type='INVALID_METADATA', domain=domain, source=source, object_key=meta_key, detail=str(exc)))

    for raw_key in raw_keys:
        domain, source = classification(raw_key)
        if raw_key not in refs:
            findings.append(dict(type='UNREFERENCED_RAW', domain=domain, source=source, object_key=raw_key, detail='No metadata raw_object_key points here'))
        try:
            sha = hashlib.sha256(read_object(client, bucket, raw_key)).hexdigest()
            sha_to_keys[sha].append(raw_key)
        except Exception as exc:
            findings.append(dict(type='RAW_READ_ERROR', domain=domain, source=source, object_key=raw_key, detail=str(exc)))

    for sha, keys in sha_to_keys.items():
        if len(keys) > 1:
            for key in keys[1:]:
                domain, source = classification(key)
                findings.append(dict(type='DUPLICATE_CONTENT', domain=domain, source=source, object_key=key,
                                     detail=f'sha256={sha}; canonical={keys[0]}'))

    for key in legacy_keys:
        domain, source = classification(key)
        findings.append(dict(type='LEGACY_LAYOUT', domain=domain, source=source, object_key=key,
                             detail='Object key has no source= segment; not necessarily invalid'))

    # Also report metadata that points outside the audited prefix.
    for raw_key, owners in refs.items():
        if len(owners) > 1:
            domain, source = classification(raw_key)
            findings.append(dict(type='MULTIPLE_METADATA_REFS', domain=domain, source=source, object_key=raw_key,
                                 detail=f'{len(owners)} metadata files reference this raw object'))

    summary = {
        'audited_at_utc': datetime.now(timezone.utc).isoformat(),
        'bucket': bucket,
        'prefix': args.prefix,
        'total_objects': len(objects),
        'metadata_objects': len(metadata_keys),
        'valid_metadata': metadata_valid,
        'raw_objects_in_standard_layout': len(raw_keys),
        'raw_referenced_by_metadata': len(set(raw_keys).intersection(refs)),
        'raw_checked_against_metadata': checked,
        'raw_integrity_pass': matched,
        'unreferenced_raw': sum(1 for f in findings if f['type'] == 'UNREFERENCED_RAW'),
        'unique_raw_contents': len(sha_to_keys),
        'duplicate_raw_copies': sum(len(v) - 1 for v in sha_to_keys.values()),
        'legacy_layout_objects': len(legacy_keys),
        'finding_counts': dict(sorted(Counter(f['type'] for f in findings).items())),
        'note': 'Legacy-layout objects are inventoried but not checksum-validated unless referenced by metadata.'
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    with (output_dir / 'findings.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['type', 'domain', 'source', 'object_key', 'detail'])
        writer.writeheader()
        writer.writerows(findings)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f'REPORTS: {output_dir / "summary.json"} | {output_dir / "findings.csv"}')
    print('READ-ONLY: No MinIO objects were changed.')


if __name__ == '__main__':
    main()
