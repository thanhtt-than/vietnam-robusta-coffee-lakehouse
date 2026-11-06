# src/bronze/crawl_vicofa.py
import json
import random
import re
import time
from pathlib import Path
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

BASE = "https://vicofa.org.vn/"
LIST_URL = BASE + "tin-thi-truong-hang-ngay-b42.html"
TOTAL_PAGES = 122
OUT_DIR = Path("data/bronze/vicofa")
OUT_DIR.mkdir(parents=True, exist_ok=True)
LIST_FILE = OUT_DIR / "article_list.jsonl"
DETAIL_FILE = OUT_DIR / "articles.jsonl"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Referer": BASE,
}

session = requests.Session()
session.headers.update(HEADERS)


def polite_sleep(a=2.0, b=4.0):
    time.sleep(random.uniform(a, b))


def get(url, retries=3):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.text
            print(f"[{r.status_code}] {url} (lần {attempt})")
            if r.status_code in (403, 429):
                time.sleep(10 * attempt)
        except requests.RequestException as e:
            print(f"[ERR] {url}: {e} (lần {attempt})")
            time.sleep(5 * attempt)
    return None

def check_robots():
    rp = RobotFileParser(BASE + "robots.txt")
    try:
        rp.read()
        print("robots.txt cho phép:", rp.can_fetch(HEADERS["User-Agent"], LIST_URL))
    except Exception as e:
        print("Không đọc được robots.txt:", e)


# ---------- Giai đoạn 1: lấy danh sách bài ----------
def parse_list(html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for li in soup.select(".baiviet_box ul > li"):
        a = li.select_one("h2.tieude a")
        if not a:
            continue
        href = urljoin(BASE, a["href"])
        m = re.search(r"-bid(\d+)\.html", href)
        date = li.select_one(".ngay .giatri")
        desc = li.select_one(".mota")
        items.append({
            "id": int(m.group(1)) if m else None,
            "title": a.get_text(strip=True),
            "url": href,
            "date": date.get_text(strip=True) if date else None,
            "summary": desc.get_text(" ", strip=True) if desc else None,
        })
    return items


def crawl_list():
    seen = set()
    if LIST_FILE.exists():
        for line in LIST_FILE.read_text(encoding="utf-8").splitlines():
            seen.add(json.loads(line)["url"])

    with LIST_FILE.open("a", encoding="utf-8") as f:
        for p in range(1, TOTAL_PAGES + 1):
            url = LIST_URL if p == 1 else f"{LIST_URL}?p={p}"
            parsed = []
            for attempt in range(3):
                html = get(url)
                parsed = parse_list(html) if html else []
                if parsed:
                    break
                time.sleep(10 * (attempt + 1))

            new = [i for i in parsed if i["url"] not in seen]
            for it in new:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
                seen.add(it["url"])
            f.flush()
            first = parsed[0]["title"][:40] if parsed else None
            print(f"Trang {p}/{TOTAL_PAGES}: tải {len(parsed)}, mới +{len(new)} "
                  f"(tổng {len(seen)}) | đầu: {first}")
            polite_sleep(3, 6)


# ---------- Giai đoạn 2: lấy nội dung từng bài ----------
CONTENT_SELECTORS = [
    ".noidung_baiviet", ".chitiet_baiviet", ".baiviet_chitiet",
    ".noidung", ".html_content", "article",
]


def parse_detail(html):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "iframe", "noscript"]):
        t.decompose()

    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else None

    content = None
    for sel in CONTENT_SELECTORS:
        node = soup.select_one(sel)
        if node and len(node.get_text(strip=True)) > 200:
            content = node
            break
    if content is None:  # fallback: khối có nhiều thẻ <p> nhất
        best, best_len = None, 0
        for div in soup.find_all("div"):
            n = sum(len(p.get_text(strip=True)) for p in div.find_all("p", recursive=False))
            if n > best_len:
                best, best_len = div, n
        content = best

    text = content.get_text("\n", strip=True) if content else None
    date = soup.select_one(".ngay .giatri")
    return {
        "title_detail": title,
        "date_detail": date.get_text(strip=True) if date else None,
        "content": text,
    }


def crawl_details():
    done = set()
    if DETAIL_FILE.exists():
        for line in DETAIL_FILE.read_text(encoding="utf-8").splitlines():
            done.add(json.loads(line)["url"])

    items = [json.loads(l) for l in LIST_FILE.read_text(encoding="utf-8").splitlines()]
    todo = [i for i in items if i["url"] not in done]
    print(f"Cần crawl {len(todo)} bài (đã xong {len(done)})")

    with DETAIL_FILE.open("a", encoding="utf-8") as f:
        for n, it in enumerate(todo, 1):
            html = get(it["url"])
            if not html:
                continue
            it.update(parse_detail(html))
            it["source"] = "vicofa.org.vn"
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
            f.flush()
            print(f"[{n}/{len(todo)}] {it['title'][:70]}")
            polite_sleep()


if __name__ == "__main__":
    check_robots()
    crawl_list()
    crawl_details()