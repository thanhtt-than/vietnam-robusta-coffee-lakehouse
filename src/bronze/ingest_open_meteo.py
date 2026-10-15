"""Incremental Open-Meteo archive -> MinIO Bronze. Run from project root with -m."""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
import re
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import requests
from src.bronze.minio_client import get_minio_client, get_bucket_name
from src.bronze.metadata import upload_bytes_and_metadata

API = 'https://archive-api.open-meteo.com/v1/archive'
VARIABLES = 'weather_code,temperature_2m_mean,temperature_2m_min,temperature_2m_max,rain_sum,precipitation_sum,et0_fao_evapotranspiration'
STATIONS = {
    'dak_nong': (12.4431, 107.9654, 'open-meteo-12.48N107.97E419m_DakNong.csv'),
    'dak_lak': (12.83, 108.13, 'open-meteo-12.83N108.13E481m_DaKLak.csv'),
    'gia_lai': (13.9833, 108.0, None),
    'lam_dong': (11.9465, 108.4419, None),
}
# Gia Lai and Lam Dong use corrected reference locations; old CSVs must not be joined to these series.

def last_csv_day(path: Path) -> date:
    if not path.exists():
        raise FileNotFoundError(f'Historical CSV not found: {path}')
    latest = None
    with path.open('r', encoding='utf-8-sig', errors='replace', newline='') as f:
        for line in f:
            first = line.split(',', 1)[0].strip().strip('"')
            if re.fullmatch(r'\d{4}-\d{2}-\d{2}', first):
                try:
                    day = date.fromisoformat(first)
                except ValueError:
                    continue
                if latest is None or day > latest:
                    latest = day
    if latest is None:
        raise ValueError(f'No YYYY-MM-DD dates found in first CSV column: {path}')
    return latest

def verified_existing(client, bucket, station):
    days = set()
    suffix = '_corrected_v2' if station in ('gia_lai', 'lam_dong') else ''
    prefix = f'bronze/weather/source=open_meteo_archive_{station}{suffix}/'
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith('/metadata.json'):
            continue
        response = client.get_object(bucket, obj.object_name)
        try:
            meta = json.loads(response.read())
        finally:
            response.close(); response.release_conn()
        if meta.get('status') != 'success':
            continue
        info = meta.get('source_metadata') or {}
        if not info.get('date_from') or not info.get('date_to'):
            continue
        try:
            stat = client.stat_object(bucket, meta['raw_object_key'])
        except Exception:
            continue
        if stat.size != meta.get('size_bytes'):
            continue
        start, end = date.fromisoformat(info['date_from']), date.fromisoformat(info['date_to'])
        for i in range((end - start).days + 1):
            days.add(start + timedelta(days=i))
    return days

def ranges(days):
    days = sorted(days)
    if not days:
        return []
    output = []
    start = previous = days[0]
    for day in days[1:]:
        if day != previous + timedelta(days=1):
            output.append((start, previous))
            start = day
        previous = day
    output.append((start, previous))
    return output

def ingest_station(args, station):
    lat, lon, filename = STATIONS[station]
    # Corrected coordinates require a new full history, not a continuation of invalid CSVs.
    start = (date.fromisoformat(args.start_date) if args.start_date else
             last_csv_day(Path(args.data_dir) / filename) + timedelta(days=1) if filename else date(2000, 1, 1))
    end = date.fromisoformat(args.end_date) if args.end_date else datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).date()
    if end < start:
        print(f'NO_NEW_DAYS: historical last day >= requested end ({end})'); return
    client = get_minio_client(); bucket = get_bucket_name()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f'MinIO bucket not found: {bucket}')
    existing = verified_existing(client, bucket, station)
    # Refresh the most recent N days, even if previously ingested. Bronze is append-only:
    # each refresh writes a new version with its own ingestion timestamp/batch ID.
    refresh_start = end - timedelta(days=args.refresh_days - 1) if args.refresh_days else end + timedelta(days=1)
    missing = [start + timedelta(days=i) for i in range((end-start).days+1)
               if start + timedelta(days=i) not in existing or start + timedelta(days=i) >= refresh_start]
    refresh_count = sum(1 for day in missing if day in existing)
    jobs = []
    for range_start, range_end in ranges(missing):
        cursor = range_start
        while cursor <= range_end:
            last_chunk = min(cursor + timedelta(days=args.chunk_days - 1), range_end)
            jobs.append((cursor, last_chunk))
            cursor = last_chunk + timedelta(days=1)
    print(f'Station={station}, coords=({lat}, {lon}), requested={start}..{end}, fetch_days={len(missing)}, refresh_days={refresh_count}')
    for first, last in jobs:
        print(f'{"WOULD_FETCH" if args.dry_run else "FETCH"}: {first}..{last}')
        if args.dry_run:
            continue
        params = {'latitude': lat, 'longitude': lon, 'start_date': first.isoformat(), 'end_date': last.isoformat(), 'daily': VARIABLES, 'timezone': 'Asia/Ho_Chi_Minh'}
        response = None
        for attempt in range(1, 5):
            try:
                response = requests.get(API, params=params, timeout=90)
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == 4 or (response is not None and response.status_code not in (429, 500, 502, 503, 504)):
                    raise
                delay = min(2 ** attempt, 20)
                print(f'RETRY {attempt}/4: {exc}; waiting {delay}s')
                time.sleep(delay)
        payload = response.json()
        if payload.get('error'):
            raise RuntimeError(f'Open-Meteo error: {payload}')
        received = payload.get('daily', {}).get('time', [])
        expected = [(first + timedelta(days=i)).isoformat() for i in range((last-first).days+1)]
        if received != expected:
            raise RuntimeError(f'Incomplete/unexpected dates: expected {len(expected)}, got {len(received)}. Not uploading.')
        for field in VARIABLES.split(','):
            values = payload.get('daily', {}).get(field)
            if not isinstance(values, list) or len(values) != len(expected):
                raise RuntimeError(f'Missing/invalid daily field {field}; not uploading.')
        raw = response.content
        name = f'{station}_{first}_{last}.json'
        result = upload_bytes_and_metadata(raw_bytes=raw, raw_filename=name, domain='weather', source=f"open_meteo_archive_{station}{'_corrected_v2' if filename is None else ''}", source_type='api', source_url=response.url, content_type='application/json', extra_metadata={'station':station, 'latitude':lat, 'longitude':lon, 'date_from':str(first), 'date_to':str(last), 'timezone':'Asia/Ho_Chi_Minh', 'record_count':len(received), 'grid_latitude':payload.get('latitude'), 'grid_longitude':payload.get('longitude'), 'grid_elevation':payload.get('elevation'), 'coordinate_version':'corrected_v2' if filename is None else 'legacy_continuation_v1', 'is_refresh': first <= end and last >= refresh_start, 'contains_current_day': first <= datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).date() <= last})
        obj = client.get_object(bucket, result['raw_object_key'])
        try:
            digest = hashlib.sha256(obj.read()).hexdigest()
        finally:
            obj.close(); obj.release_conn()
        if digest != result['sha256']:
            raise RuntimeError('Read-back SHA256 mismatch')
        print(f'SUCCESS: {len(received)} days; {result["raw_object_key"]}')
        time.sleep(1)
    if not jobs:
        print('SKIPPED: all requested days already ingested (outside refresh window)')

def resolve_default_end_date():
    """Use today when Archive supports it; otherwise use its reported latest date."""
    today = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).date()
    params = {
        'latitude': STATIONS['dak_nong'][0],
        'longitude': STATIONS['dak_nong'][1],
        'start_date': (today - timedelta(days=7)).isoformat(),
        'end_date': today.isoformat(),
        'daily': VARIABLES,
        'timezone': 'Asia/Ho_Chi_Minh',
    }
    response = requests.get(API, params=params, timeout=90)
    if response.ok:
        payload = response.json()
        if payload.get('daily', {}).get('time', [])[-1:] == [today.isoformat()]:
            print(f'ARCHIVE_END_DATE: {today} (today is available)')
            return today
        raise RuntimeError('Archive probe returned unexpected dates; refusing to guess the cutoff')
    try:
        reason = response.json().get('reason', '')
    except ValueError:
        reason = response.text[:300]
    # Probe begins seven days ago to avoid an invalid start_date when today is unavailable.
    # Only handle the end_date range error; other 400s must be visible.
    match = re.search(r"Parameter 'end_date' is out of allowed range from \d{4}-\d{2}-\d{2} to (\d{4}-\d{2}-\d{2})", reason)
    if response.status_code == 400 and match:
        cutoff = date.fromisoformat(match.group(1))
        if cutoff > today:
            raise RuntimeError(f'Unexpected future Archive cutoff: {cutoff}')
        print(f'ARCHIVE_END_DATE: {cutoff} (today {today} not yet supported)')
        return cutoff
    raise RuntimeError(f'Archive end-date probe failed: HTTP {response.status_code}: {reason}')

def main():
    p = argparse.ArgumentParser(description='Incremental Open-Meteo -> MinIO Bronze (4 stations)')
    p.add_argument('--station', required=True, choices=[*STATIONS, 'all'])
    p.add_argument('--data-dir', default=r'D:\TLCN\data')
    p.add_argument('--start-date', help='Override start date, YYYY-MM-DD (use with care)')
    p.add_argument('--end-date', help='Default: latest date accepted by Archive API, up to today in Asia/Ho_Chi_Minh')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--chunk-days', type=int, default=365)
    p.add_argument('--refresh-days', type=int, default=3, help='Re-fetch the last N days; 0 disables refresh')
    args = p.parse_args()
    if not 1 <= args.chunk_days <= 365:
        p.error('--chunk-days must be 1..365')
    if not 0 <= args.refresh_days <= 30:
        p.error('--refresh-days must be 0..30')
    if args.end_date is None:
        args.end_date = resolve_default_end_date().isoformat()
    stations = list(STATIONS) if args.station == 'all' else [args.station]
    failed = []
    for station in stations:
        print(f'\n===== {station.upper()} =====', flush=True)
        try:
            ingest_station(args, station)
        except Exception as exc:
            print(f'ERROR [{station}]: {type(exc).__name__}: {exc}', flush=True)
            failed.append(station)
            if args.station != 'all':
                raise
    print(f'\nSUMMARY: stations={len(stations)}, succeeded={len(stations)-len(failed)}, failed={len(failed)}')
    if failed:
        raise SystemExit(f'Failed stations: {", ".join(failed)}')

if __name__ == '__main__':
    main()
