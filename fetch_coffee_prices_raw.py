"""
fetch_coffee_prices_raw.py
===========================
Bản RÚT GỌN: chỉ thu thập (ingest) dữ liệu thô từ API bbw.vn và lưu nguyên
văn JSON xuống ổ đĩa. KHÔNG làm sạch, KHÔNG khử trùng lặp, KHÔNG chia lớp
Bronze/Silver/Gold — để dành cho các bước sau.

API:
    GET https://bbw.vn/api/proxy/commodity-spot-prices
        ?product=COFFEE&limit=100&sort=newest&from=YYYY-MM-DD&to=YYYY-MM-DD
        &snapshot_mode=none

Cách chạy:
    python fetch_coffee_prices_raw.py

Dữ liệu sẽ được lưu vào thư mục "raw_data" NẰM NGAY CẠNH file script này
(đường dẫn tương đối, không phụ thuộc /mnt/... hay hệ điều hành).
Ví dụ nếu script ở D:\\TLCN\\fetch_coffee_prices_raw.py thì dữ liệu nằm ở
D:\\TLCN\\raw_data\\
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

# ============================== CONFIG ======================================

BASE_URL = "https://bbw.vn/api/proxy/commodity-spot-prices"
PRODUCT = "COFFEE"
PAGE_LIMIT = 100
SORT = "newest"
SNAPSHOT_MODE = "none"

TODAY = date.today()

# Mốc an toàn: không lùi về trước ngày này (tránh gọi API vô hạn).
# Cứ để rộng, script tự dừng khi hết dữ liệu thật.
MIN_DATE_FLOOR = date(2015, 1, 1)

MAX_ITERATIONS = 500
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS = 0.6

# Thư mục lưu dữ liệu thô -> nằm NGAY CẠNH file script này (dễ tìm)
RAW_DIR = Path(__file__).resolve().parent / "raw_data" / f"coffee_{TODAY.isoformat()}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ============================== FETCH LOGIC =================================


def fetch_page(from_date: date, to_date: date, limit: int = PAGE_LIMIT,
                max_retries: int = 3) -> dict:
    params = {
        "product": PRODUCT,
        "limit": limit,
        "sort": SORT,
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "snapshot_mode": SNAPSHOT_MODE,
    }

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(BASE_URL, params=params, headers=HEADERS,
                                 timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = 2 ** attempt
            print(f"  [WARN] Lỗi gọi API (lần {attempt}/{max_retries}): {exc}. "
                  f"Thử lại sau {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Gọi API thất bại sau {max_retries} lần") from last_exc


def get_oldest_date(records: list[dict]) -> date | None:
    dates = []
    for r in records:
        qd = r.get("quote_date_vn")
        if qd:
            try:
                dates.append(datetime.strptime(qd, "%Y-%m-%d").date())
            except ValueError:
                pass
    return min(dates) if dates else None


def save_raw(payload: dict, from_date: date, to_date: date, seq: int) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"req_{seq:04d}_{from_date.isoformat()}_{to_date.isoformat()}.json"
    fpath = RAW_DIR / fname
    fpath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return fpath


def backfill_raw(end_date: date = TODAY, floor_date: date = MIN_DATE_FLOOR) -> None:
    to_date = end_date
    seq = 0
    total_records = 0

    print(f"Thư mục lưu dữ liệu thô: {RAW_DIR}\n")
    print(f"Bắt đầu thu thập từ {end_date} lùi về tối đa {floor_date} ...\n")

    while seq < MAX_ITERATIONS:
        seq += 1
        from_date = floor_date
        print(f"[{seq}] Gọi API: from={from_date} to={to_date} limit={PAGE_LIMIT}")

        payload = fetch_page(from_date=from_date, to_date=to_date)
        records = payload.get("data", []) or []
        fpath = save_raw(payload, from_date, to_date, seq)

        n = len(records)
        total_records += n
        print(f"    -> nhận {n} bản ghi, đã lưu: {fpath.name}")

        if n == 0:
            print("    Không còn dữ liệu trong khoảng này -> dừng.")
            break

        oldest = get_oldest_date(records)
        if oldest is None:
            print("    Không xác định được ngày cũ nhất trong batch -> dừng để an toàn.")
            break

        if oldest <= floor_date:
            print(f"    Đã chạm mốc an toàn {floor_date} -> dừng.")
            break

        new_to_date = oldest - timedelta(days=1)
        if new_to_date >= to_date:
            print("    Không có tiến triển về thời gian -> dừng để tránh lặp vô hạn.")
            break
        to_date = new_to_date

        if n < PAGE_LIMIT:
            print("    Số bản ghi nhận được < limit -> có thể đã hết dữ liệu lịch sử, dừng.")
            break

        time.sleep(SLEEP_BETWEEN_CALLS)
    else:
        print(f"[WARN] Đã chạm MAX_ITERATIONS={MAX_ITERATIONS}.")

    print(f"\nHOÀN TẤT. Tổng số request: {seq}, tổng số bản ghi thô: {total_records}")
    print(f"Toàn bộ file JSON nằm tại: {RAW_DIR}")


if __name__ == "__main__":
    backfill_raw()
