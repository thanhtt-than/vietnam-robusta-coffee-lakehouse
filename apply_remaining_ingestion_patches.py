"""Apply the small, guarded Silver/DAG compatibility edits for this patch.

Run once from the TLCN repository root:
    python .\apply_remaining_ingestion_patches.py

The script makes .bak copies and refuses to patch unexpected file contents.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent


def replace_once(path: Path, old: str, new: str, label: str) -> None:
    text = path.read_text(encoding="utf-8-sig")
    if new in text:
        print(f"ALREADY_PATCHED: {label}")
        return
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"Cannot safely patch {label}: expected one exact match, found {count}. "
            "No file was changed by this replacement."
        )
    backup = path.with_suffix(path.suffix + ".before_ingestion_patch.bak")
    if not backup.exists():
        backup.write_text(text, encoding="utf-8")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    print(f"PATCHED: {label}; backup={backup}")


def main() -> None:
    silver = Path("src/iceberg/silver_vicofa_articles.py")
    dag = Path("infrastructure/airflow-v3/dags/tlcn_full_lakehouse_pipeline.py")
    if not silver.is_file() or not dag.is_file():
        raise FileNotFoundError(
            "Run this script from the TLCN repository root; required Silver/DAG files were not found."
        )

    replace_once(
        silver,
        """        if total != 2436:\n            raise RuntimeError(f'Unexpected Bronze VICOFA rows: {total}; no writes')\n""",
        """        list_count = raw.filter(F.col('record_type') == 'vicofa_article_list').count()\n        detail_count = raw.filter(F.col('record_type') == 'vicofa_article_detail').count()\n        print('VICOFA_LIST_DETAIL_COUNTS:', list_count, detail_count, flush=True)\n        if (total != list_count + detail_count or list_count == 0\n                or detail_count == 0 or list_count != detail_count):\n            raise RuntimeError(\n                f'Unexpected Bronze VICOFA list/detail counts: {list_count}/{detail_count}; no writes'\n            )\n""",
        "Silver Bronze row-count guard",
    )

    replace_once(
        silver,
        """            if count != 1218 or unique != count or invalid:\n                raise RuntimeError(f'{name}: unexpected/missing/duplicate ids; no writes')\n""",
        """            if count == 0 or unique != count or invalid:\n                raise RuntimeError(f'{name}: empty/missing/duplicate ids; no writes')\n""",
        "Silver fixed 1,218-row guard",
    )

    replace_once(
        silver,
        """        if rows != 1218 or unique != rows:\n            raise RuntimeError(f'Expected 1218 unique article ids, got {rows}/{unique}')\n""",
        """        if rows == 0 or rows != list_count or rows != detail_count or unique != rows:\n            raise RuntimeError(\n                f'Expected one canonical row per paired VICOFA ID; got rows={rows}, '\n                f'list={list_count}, detail={detail_count}, unique={unique}'\n            )\n""",
        "Silver fixed canonical-row guard",
    )

    replace_once(
        silver,
        """            stored_total = stored.count()\n            stored_unique = stored.select('article_id').distinct().count()\n            print('SILVER_VICOFA_TOTAL:', stored_total, flush=True)\n            if stored_total != 1218 or stored_unique != stored_total:\n                raise RuntimeError(f'VICOFA Iceberg verification failed {stored_total}/{stored_unique}')\n""",
        """            stored_total = stored.count()\n            stored_unique = stored.select('article_id').distinct().count()\n            current_ids = result.select('article_id').distinct()\n            current_ids_present = stored.join(current_ids, 'article_id', 'inner').count()\n            print('SILVER_VICOFA_TOTAL:', stored_total, flush=True)\n            print('SILVER_VICOFA_CURRENT_SOURCE_IDS_PRESENT:', current_ids_present, flush=True)\n            if current_ids_present != rows or stored_unique != stored_total:\n                raise RuntimeError(\n                    f'VICOFA Iceberg verification failed: total={stored_total}, '\n                    f'unique={stored_unique}, current_source_present={current_ids_present}/{rows}'\n                )\n""",
        "Silver Iceberg fixed 1,218-row write guard",
    )

    replace_once(
        dag,
        """docker exec {CONTAINER} test -f {GUARD_PATH}\necho TLCN_PIPELINE_PREFLIGHT_PASSED""",
        """docker exec {CONTAINER} test -f {GUARD_PATH}\ndocker exec {CONTAINER} test -f /workspace/data/open-meteo-12.48N107.97E419m_DakNong.csv\ndocker exec {CONTAINER} test -f /workspace/data/open-meteo-12.83N108.13E481m_DaKLak.csv\ndocker exec -w /workspace -e PYTHONPATH=/workspace {CONTAINER} python3 -c 'import requests, bs4, minio, dotenv; from src.bronze.minio_client import get_minio_client, get_bucket_name; assert get_minio_client().bucket_exists(get_bucket_name()), "MinIO bucket unavailable"'\necho TLCN_INGESTION_PREFLIGHT_PASSED\necho TLCN_PIPELINE_PREFLIGHT_PASSED""",
        "Airflow ingestion dependency/MinIO/data preflight",
    )

    replace_once(
        dag,
        """            task("crawl_vicofa", python_job("src/bronze/crawl_vicofa.py"), 45),""",
        """            task("crawl_vicofa", python_job(\n                "src/bronze/crawl_vicofa.py", ["--allow-stale"]), 45),""",
        "Airflow VICOFA stale-baseline fallback flag",
    )

    print("REMAINING_INGESTION_PATCHES_APPLIED", flush=True)


if __name__ == "__main__":
    main()
