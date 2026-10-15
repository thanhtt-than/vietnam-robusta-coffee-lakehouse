"""
fetch_giacaphe_com.py
=======================
Thu thập dữ liệu giá cà phê từ trang: https://giacaphe.com/gia-ca-phe-noi-dia/

LƯU Ý QUAN TRỌNG VỀ TRANG NÀY:
- Đây KHÔNG phải là một REST API JSON công khai (không giống bbw.vn). Trang trả
  về HTML render sẵn (server-side render qua WordPress).
- File update-quotes.min.js chỉ là JS tĩnh (cache Cloudflare), không phải nơi
  chứa dữ liệu.
- Bảng "Giá cà phê trong nước" theo tỉnh bị ẩn số bằng kỹ thuật CSS:
      HTML:  <span class="SjNmrgf UkdrxwG"></span>   (rỗng, không có số)
      CSS:   .SjNmrgf::after{ content:'94,700'; }    (số thật nằm trong CSS)
  Tên class là chuỗi ngẫu nhiên, CÓ THỂ THAY ĐỔI MỖI LẦN TẢI TRANG, nên
  không được hardcode tên class — phải luôn parse lại cặp (class -> giá trị)
  từ khối <style> của mỗi lần tải trang.
- Bảng "Giá cà phê Robusta London" thì KHÔNG bị ẩn, số nằm thẳng trong text
  (và có sẵn thuộc tính data-price) nên dễ lấy hơn.

Script này lấy cả 2 bảng + giá trung bình + ngày cập nhật, lưu:
  - HTML thô (để đối chiếu/backup)
  - JSON đã trích xuất (raw_data/giacaphe/<ngày>/data.json)

Cách chạy:
    python fetch_giacaphe_com.py
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ============================== CONFIG ======================================

URL = "https://giacaphe.com/gia-ca-phe-noi-dia/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TODAY = date.today()
OUT_DIR = Path(__file__).resolve().parent / "raw_data" / "giacaphe" / TODAY.isoformat()

# ============================== HELPERS ======================================


def fetch_html(url: str = URL, timeout: int = 20) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def build_css_value_map(soup: BeautifulSoup) -> dict[str, str]:
    """
    Đọc toàn bộ <style> trong trang, trả về map:
        { class_name: value_string }
    cho các rule dạng .CLASS::after{ ... content:'VALUE'; ... }

    Bỏ qua các class "decoy" có content rỗng hoặc chỉ có khoảng trắng
    (như .UkdrxwG dùng chung, không mang giá trị thật).
    """
    style_text = " ".join(tag.get_text() for tag in soup.find_all("style"))
    pattern = re.compile(r"\.([A-Za-z0-9_-]+)::after\s*\{[^}]*content:\s*'([^']*)'")
    raw_map = dict(pattern.findall(style_text))
    return {k: v for k, v in raw_map.items() if v.strip() not in ("", "&nbsp;")}


def resolve_span_value(span, value_map: dict[str, str]) -> str | None:
    """Với 1 thẻ <span class="X Y">, tìm class nào có trong value_map và trả giá trị."""
    if span is None:
        return None
    # Nếu span có sẵn text (trường hợp không bị ẩn) thì ưu tiên dùng luôn
    text = span.get_text(strip=True)
    if text:
        return text
    for cls in span.get("class", []):
        if cls in value_map:
            return value_map[cls]
    return None


# ============================== PARSERS ======================================


def parse_update_date(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1", class_="page-title")
    if h1:
        return h1.get_text(strip=True)
    return None


def parse_average_price(soup: BeautifulSoup) -> dict:
    box = soup.find("div", class_="gia--trung-binh")
    if not box:
        return {}
    gia_span = box.find("span", class_="_trung-binh-gia")
    change_span = box.find("span", class_="_change")
    gia_text = None
    if gia_span:
        # Bỏ phần đơn vị "đ/kg" lồng bên trong
        dvt = gia_span.find("span", class_="_gia--dvt")
        dvt_text = dvt.get_text(strip=True) if dvt else ""
        full_text = gia_span.get_text(strip=True)
        gia_text = full_text.replace(dvt_text, "").strip()
    return {
        "gia_trung_binh": gia_text,
        "thay_doi": change_span.get_text(strip=True) if change_span else None,
        "thay_doi_raw_price": change_span.get("data-price") if change_span else None,
    }


def parse_domestic_table(soup: BeautifulSoup, value_map: dict[str, str]) -> list[dict]:
    """Bảng 'Giá cà phê trong nước' theo tỉnh -- bảng bị ẩn số qua CSS."""
    container = soup.find("div", id="gia-noi-dia")
    if not container:
        return []

    rows_out = []
    for tr in container.select("table.price-table tbody tr"):
        if "tb_tr_credit" in (tr.get("class") or []):
            continue
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        name = tds[0].get_text(strip=True)
        price = resolve_span_value(tds[1].find("span"), value_map)
        change = resolve_span_value(tds[2].find("span"), value_map)

        rows_out.append({
            "thi_truong": name,
            "gia": price,
            "thay_doi": change,
        })
    return rows_out


def parse_robusta_london(soup: BeautifulSoup) -> list[dict]:
    """Bảng 'Giá cà phê Robusta London' -- KHÔNG bị ẩn, lấy trực tiếp từ text/data-price."""
    section = soup.find("section", id="robusta-london")
    if not section:
        return []

    rows_out = []
    for tr in section.select("table.livequote tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 9:
            continue

        ky_han = tds[0].get_text(strip=True)
        gia_khop = tds[1].get_text(strip=True)
        gia_khop_raw = tds[1].get("data-price")

        change_span = tds[2].find("span", class_="price_change")
        pct_span = tds[2].find("span", class_="pctchange")

        high_spans = tds[3].find_all("span")
        low_spans = tds[4].find_all("span")

        rows_out.append({
            "ky_han": ky_han,
            "gia_khop": gia_khop,
            "gia_khop_raw": gia_khop_raw,
            "thay_doi": change_span.get_text(strip=True) if change_span else None,
            "thay_doi_pct": pct_span.get_text(strip=True) if pct_span else None,
            "cao_nhat": high_spans[0].get_text(strip=True) if high_spans else None,
            "thap_nhat": low_spans[0].get_text(strip=True) if low_spans else None,
            "khoi_luong": tds[5].get_text(strip=True),
            "mo_cua": tds[6].get_text(strip=True),
            "hom_truoc": tds[7].get_text(strip=True),
            "hd_mo": tds[8].get_text(strip=True),
        })
    return rows_out


# ============================== MAIN =========================================


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Đang tải trang: {URL}")
    html = fetch_html()

    # Lưu HTML thô để backup / đối chiếu
    raw_html_path = OUT_DIR / "raw_page.html"
    raw_html_path.write_text(html, encoding="utf-8")
    print(f"Đã lưu HTML thô tại: {raw_html_path}")

    soup = BeautifulSoup(html, "html.parser")
    value_map = build_css_value_map(soup)
    print(f"Tìm được {len(value_map)} class chứa giá trị ẩn trong CSS.")

    result = {
        "fetched_at": TODAY.isoformat(),
        "source_url": URL,
        "tieu_de": parse_update_date(soup),
        "gia_trung_binh": parse_average_price(soup),
        "gia_noi_dia_theo_tinh": parse_domestic_table(soup, value_map),
        "gia_robusta_london": parse_robusta_london(soup),
    }

    out_json = OUT_DIR / "data.json"
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n--- KẾT QUẢ ---")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\nĐã lưu JSON tại: {out_json}")


if __name__ == "__main__":
    main()
