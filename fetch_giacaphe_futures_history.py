"""
fetch_giacaphe_futures_history.py
====================================
Thu thập LỊCH SỬ giá 2 sàn: Robusta London (ICE Futures Europe) và
Arabica New York (ICE Futures US) từ giacaphe.com.

Ý tưởng:
- Trang https://giacaphe.com/gia-ca-phe-noi-dia/ có 1 danh sách "Giá cà phê
  những ngày khác" trỏ tới URL dạng:
      https://giacaphe.com/gia-ca-phe-noi-dia-ngay-YYYY-MM-DD/
  Mỗi URL này là trang lưu trữ đầy đủ dữ liệu của đúng ngày đó, bao gồm cả
  2 bảng Robusta London và Arabica New York (2 bảng này KHÔNG bị ẩn số như
  bảng giá nội địa theo tỉnh).
- Script sẽ lùi dần từng ngày từ hôm nay về quá khứ, tải từng trang theo
  URL trên, trích 2 bảng cần thiết, bỏ qua toàn bộ phần còn lại
  (giá nội địa theo tỉnh, tỷ giá, hồ tiêu, bài viết, v.v.)

Vì có thể có ngày không tồn tại trang lưu trữ (nghỉ lễ, cuối tuần thị
trường không giao dịch, hoặc trước ngày site bắt đầu lưu dữ liệu), script
sẽ:
  - Coi HTTP 404 là "không có dữ liệu ngày này", KHÔNG dừng ngay, chỉ đếm
    dồn số ngày liên tiếp không có dữ liệu.
  - Dừng backfill khi số ngày liên tiếp không có dữ liệu vượt ngưỡng
    MAX_CONSECUTIVE_MISS (mặc định 60 ngày ~ 2 tháng) -> khả năng cao đã
    lùi quá điểm bắt đầu lưu trữ của site.
  - Có thêm giới hạn an toàn MAX_TOTAL_DAYS để tránh chạy vô hạn.

Dữ liệu được ghi APPEND từng dòng vào 1 file .jsonl (mỗi dòng là dữ liệu
1 ngày) để nếu chương trình bị dừng giữa chừng (mất mạng, tắt máy...) thì
lần chạy sau vẫn giữ được toàn bộ dữ liệu đã lấy, và có thể chạy lại để
tiếp tục (script tự bỏ qua ngày đã có sẵn trong file).

Cách dùng:
    python fetch_giacaphe_futures_history.py
"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ============================== CONFIG ======================================

START_DATE = date.today()          # bắt đầu backfill từ hôm nay, lùi dần
MAX_CONSECUTIVE_MISS = 60          # số ngày liên tiếp không có trang -> dừng
MAX_TOTAL_DAYS = 4000              # giới hạn an toàn tuyệt đối (~11 năm)
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS = 1.0          # giây, lịch sự với server (chạy lâu, đừng vội)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

OUT_DIR = Path(__file__).resolve().parent / "raw_data" / "giacaphe_futures"
OUT_JSONL = OUT_DIR / "robusta_arabica_history.jsonl"

# Đặt True nếu muốn giữ thêm HTML thô từng ngày (tốn dung lượng hơn nhiều
# vì chạy lâu dài, mặc định tắt vì chỉ cần 2 bảng số liệu).
SAVE_RAW_HTML = False


def url_for_date(d: date) -> str:
    return f"https://giacaphe.com/gia-ca-phe-noi-dia-ngay-{d.isoformat()}/"


# ============================== FETCH / PARSE ================================


def fetch_page(url: str, max_retries: int = 3) -> requests.Response | None:
    """
    Trả về Response nếu tải thành công (bất kể status code, để mình tự xét
    404 riêng), hoặc None nếu lỗi mạng sau khi retry hết số lần.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            return resp
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"    [WARN] Lỗi mạng (lần {attempt}/{max_retries}): {exc}. "
                  f"Thử lại sau {wait}s...")
            time.sleep(wait)
    return None


def parse_futures_table(soup: BeautifulSoup, section_id: str) -> list[dict]:
    """Parse 1 trong 2 bảng livequote (Robusta London hoặc Arabica New York)."""
    section = soup.find(id=section_id)
    if not section:
        return []

    rows_out = []
    for tr in section.select("table.livequote tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 9:
            continue

        change_span = tds[2].find("span", class_="price_change")
        pct_span = tds[2].find("span", class_="pctchange")
        high_spans = tds[3].find_all("span")
        low_spans = tds[4].find_all("span")

        rows_out.append({
            "ky_han": tds[0].get_text(strip=True),
            "gia_khop": tds[1].get_text(strip=True),
            "gia_khop_raw": tds[1].get("data-price"),
            "thay_doi": change_span.get_text(strip=True) if change_span else None,
            "thay_doi_raw": change_span.get("data-price") if change_span else None,
            "thay_doi_pct": pct_span.get_text(strip=True) if pct_span else None,
            "cao_nhat": high_spans[0].get_text(strip=True) if high_spans else None,
            "thap_nhat": low_spans[0].get_text(strip=True) if low_spans else None,
            "khoi_luong": tds[5].get_text(strip=True),
            "mo_cua": tds[6].get_text(strip=True),
            "hom_truoc": tds[7].get_text(strip=True),
            "hd_mo": tds[8].get_text(strip=True),
        })
    return rows_out


def load_existing_dates(jsonl_path: Path) -> set[str]:
    """Đọc các ngày đã có sẵn trong file jsonl (để resume, khỏi tải lại)."""
    if not jsonl_path.exists():
        return set()
    dates = set()
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                dates.add(rec["date"])
            except (json.JSONDecodeError, KeyError):
                continue
    return dates


# ============================== MAIN =========================================


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    existing_dates = load_existing_dates(OUT_JSONL)
    if existing_dates:
        print(f"Đã có sẵn {len(existing_dates)} ngày trong file, sẽ bỏ qua và "
              f"tiếp tục lùi xa hơn.\n")

    current = START_DATE
    consecutive_miss = 0
    total_checked = 0
    total_saved = 0

    with OUT_JSONL.open("a", encoding="utf-8") as out_f:
        while total_checked < MAX_TOTAL_DAYS and consecutive_miss < MAX_CONSECUTIVE_MISS:
            total_checked += 1
            date_str = current.isoformat()

            if date_str in existing_dates:
                print(f"[{total_checked}] {date_str}: đã có sẵn, bỏ qua.")
                current -= timedelta(days=1)
                continue

            url = url_for_date(current)
            resp = fetch_page(url)

            if resp is None:
                print(f"[{total_checked}] {date_str}: lỗi mạng liên tục, tính là MISS.")
                consecutive_miss += 1
                current -= timedelta(days=1)
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            if resp.status_code == 404:
                print(f"[{total_checked}] {date_str}: không có trang (404) -- "
                      f"MISS liên tiếp: {consecutive_miss + 1}")
                consecutive_miss += 1
                current -= timedelta(days=1)
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            if resp.status_code != 200:
                print(f"[{total_checked}] {date_str}: status {resp.status_code}, "
                      f"tính là MISS.")
                consecutive_miss += 1
                current -= timedelta(days=1)
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            resp.encoding = resp.apparent_encoding or "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            robusta = parse_futures_table(soup, "robusta-london")
            arabica = parse_futures_table(soup, "arabica-newyork")

            if not robusta and not arabica:
                print(f"[{total_checked}] {date_str}: trang tồn tại nhưng không "
                      f"parse được bảng nào -- tính là MISS.")
                consecutive_miss += 1
                current -= timedelta(days=1)
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            # Có dữ liệu hợp lệ -> reset chuỗi miss, ghi ra file
            consecutive_miss = 0
            total_saved += 1

            record = {
                "date": date_str,
                "source_url": url,
                "robusta_london": robusta,
                "arabica_newyork": arabica,
            }
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            out_f.flush()

            if SAVE_RAW_HTML:
                day_dir = OUT_DIR / date_str
                day_dir.mkdir(parents=True, exist_ok=True)
                (day_dir / "raw_page.html").write_text(resp.text, encoding="utf-8")

            print(f"[{total_checked}] {date_str}: OK -- "
                  f"{len(robusta)} kỳ hạn Robusta, {len(arabica)} kỳ hạn Arabica")

            current -= timedelta(days=1)
            time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"\nHOÀN TẤT. Đã kiểm tra {total_checked} ngày, lưu được {total_saved} "
          f"ngày có dữ liệu.")
    if consecutive_miss >= MAX_CONSECUTIVE_MISS:
        print(f"Dừng do gặp {consecutive_miss} ngày liên tiếp không có dữ liệu "
              f"(khả năng đã lùi quá điểm bắt đầu lưu trữ của site).")
    print(f"Dữ liệu lưu tại: {OUT_JSONL}")


if __name__ == "__main__":
    main()
