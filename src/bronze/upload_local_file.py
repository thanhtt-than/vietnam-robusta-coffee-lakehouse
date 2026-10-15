"""Upload file local lên MinIO Bronze. Chạy từ thư mục gốc D:\\TLCN."""
from __future__ import annotations
import argparse
from pathlib import Path
from src.bronze.metadata import upload_bytes_and_metadata, guess_content_type


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload file local lên MinIO Bronze.")
    parser.add_argument("--file", required=True, help="Đường dẫn file trên máy")
    parser.add_argument("--domain", required=True, help="Ví dụ production, market_futures")
    parser.add_argument("--source", required=True, help="Mã nguồn, ví dụ collected_csv")
    parser.add_argument("--source-url", default=None, help="URL nguồn gốc nếu biết")
    parser.add_argument("--batch-id", default=None, help="Tùy chọn; mặc định tự sinh")
    args = parser.parse_args()

    path = Path(args.file).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {path}")
    if not path.is_file():
        raise IsADirectoryError(f"Đường dẫn không phải file: {path}")

    raw_bytes = path.read_bytes()
    result = upload_bytes_and_metadata(
        raw_bytes=raw_bytes, raw_filename=path.name, domain=args.domain,
        source=args.source, source_type="local_file", source_url=args.source_url,
        content_type=guess_content_type(path.name), batch_id=args.batch_id,
        extra_metadata={"local_filename": path.name, "local_file_size_bytes": path.stat().st_size},
    )
    print("Upload thành công.")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
