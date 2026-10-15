"""Gọi API và lưu response body gốc vào MinIO Bronze."""
from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
import requests
from src.bronze.metadata import upload_bytes_and_metadata


def parse_headers(items: list[str]) -> dict[str, str]:
    headers = {}
    for item in items:
        if ":" not in item:
            raise ValueError(f"Header không hợp lệ: {item!r}; dùng 'Tên: Giá trị'.")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def main() -> None:
    parser = argparse.ArgumentParser(description="Gọi API và lưu phản hồi gốc vào Bronze.")
    parser.add_argument("--url", required=True, help="URL API đầy đủ")
    parser.add_argument("--domain", required=True, help="Ví dụ domestic_price")
    parser.add_argument("--source", required=True, help="Ví dụ bbw_api")
    parser.add_argument("--output-name", default="response.json")
    parser.add_argument("--method", choices=["GET", "POST"], default="GET")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--header", action="append", default=[], help="Header dạng 'Tên: Giá trị'; có thể lặp")
    parser.add_argument("--params-json", default=None, help="Query params dạng JSON object")
    parser.add_argument("--body-json", default=None, help="JSON body cho POST")
    parser.add_argument("--batch-id", default=None)
    args = parser.parse_args()

    headers = parse_headers(args.header)
    params = json.loads(args.params_json) if args.params_json else None
    if params is not None and not isinstance(params, dict):
        raise ValueError("--params-json phải là JSON object.")
    body = json.loads(args.body_json) if args.body_json else None
    if body is not None and args.method != "POST":
        raise ValueError("--body-json chỉ sử dụng với --method POST.")
    if body is not None:
        headers.setdefault("Content-Type", "application/json")

    response = requests.request(args.method, args.url, headers=headers, params=params,
                                json=body, timeout=args.timeout)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0].strip()
    result = upload_bytes_and_metadata(
        raw_bytes=response.content, raw_filename=args.output_name,
        domain=args.domain, source=args.source, source_type="api",
        source_url=response.url, content_type=content_type or "application/octet-stream",
        batch_id=args.batch_id,
        extra_metadata={
            "http_method": args.method, "http_status": response.status_code,
            "response_content_type": response.headers.get("Content-Type"),
            "response_date": response.headers.get("Date"),
            "response_etag": response.headers.get("ETag"),
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "request_params": params,
        },
    )
    print(f"API ingestion thành công (HTTP {response.status_code}).")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
