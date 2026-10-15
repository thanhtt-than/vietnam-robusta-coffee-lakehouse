"""Tải và lưu nguyên response body website vào MinIO Bronze; không parse HTML."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import requests
from src.bronze.metadata import upload_bytes_and_metadata

DEFAULT_USER_AGENT = "TLCN-Coffee-Lakehouse/1.0 (academic data collection)"


def parse_headers(items: list[str]) -> dict[str, str]:
    headers = {}
    for item in items:
        if ":" not in item:
            raise ValueError(f"Header không hợp lệ: {item!r}; dùng 'Tên: Giá trị'.")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def main() -> None:
    parser = argparse.ArgumentParser(description="Tải response website và lưu bản gốc vào Bronze.")
    parser.add_argument("--url", required=True)
    parser.add_argument("--domain", required=True, help="Ví dụ company_disclosure")
    parser.add_argument("--source", required=True, help="Ví dụ nestle_vietnam")
    parser.add_argument("--output-name", default="page.html")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--header", action="append", default=[], help="Header dạng 'Tên: Giá trị'; có thể lặp")
    parser.add_argument("--batch-id", default=None)
    args = parser.parse_args()

    headers = parse_headers(args.header)
    headers.setdefault("User-Agent", args.user_agent)
    response = requests.get(args.url, headers=headers, timeout=args.timeout, allow_redirects=True)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0].strip()

    result = upload_bytes_and_metadata(
        raw_bytes=response.content, raw_filename=args.output_name,
        domain=args.domain, source=args.source, source_type="website",
        source_url=response.url, content_type=content_type or "application/octet-stream",
        batch_id=args.batch_id,
        extra_metadata={
            "requested_url": args.url, "final_url_after_redirects": response.url,
            "http_status": response.status_code,
            "response_content_type": response.headers.get("Content-Type"),
            "response_last_modified": response.headers.get("Last-Modified"),
            "response_etag": response.headers.get("ETag"),
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "redirect_count": len(response.history),
        },
    )
    print(f"Website crawl thành công (HTTP {response.status_code}).")
    print(f"Final URL: {response.url}")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
