# src/bronze/diagnose_vicofa.py
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup

BASE = "https://vicofa.org.vn/"
LIST_URL = BASE + "tin-thi-truong-hang-ngay-b42.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Referer": BASE,
}
out = Path("data/bronze/vicofa/debug"); out.mkdir(parents=True, exist_ok=True)

# 1) đọc robots.txt
r = requests.get(BASE + "robots.txt", headers=HEADERS, timeout=15)
print("robots.txt status:", r.status_code)
print(r.text[:1500])
print("=" * 60)

# 2) so sánh các trang
for p in [1, 19, 20, 21, 54, 55, 56, 100, 122]:
    url = LIST_URL if p == 1 else f"{LIST_URL}?p={p}"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")
    n = len(soup.select(".baiviet_box ul > li"))
    first = soup.select_one(".baiviet_box h2.tieude a")
    print(f"p={p:3d} status={resp.status_code} size={len(resp.text):7d} "
          f"bài={n:2d} đầu='{first.get_text(strip=True)[:50] if first else None}'")
    (out / f"page_{p}.html").write_text(resp.text, encoding="utf-8")
    time.sleep(5)