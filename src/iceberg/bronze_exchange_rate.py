"""Ingest historical FX files and GoogleFinance JSON into Bronze Iceberg.

No openpyxl dependency: .xlsx files are read with Python's standard library.
GoogleFinance's USD_VND/EUR_VND observations are mapped to the existing
analytical source codes usd_vnd_csv/eur_vnd_csv so Silver/Gold contracts remain
unchanged. The physical MinIO object_key still identifies googlefinance_sheet.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from pyspark.sql.types import StructType, StructField, StringType, LongType

from minio_direct import (
    MinioDirect,
    csv_records,
    json_record,
    record_id_for,
    spark_session,
    merge_into_bronze,
)

TABLE = "lakehouse.bronze.exchange_rate_raw"
PREFIX = "bronze/exchange_rate/"

SUPPORTED_MINIO_SOURCES = {
    "usd_vnd_csv",
    "eur_vnd_csv",
    "usd_vnd_excel",
    "eur_vnd_excel",
    "imf_exchange_rate",
    "googlefinance_sheet",
}

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("object_key", StringType(), False),
    StructField("record_position", LongType(), False),
    StructField("domain", StringType(), False),
    StructField("source", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("object_sha256", StringType(), False),
    StructField("raw_record_json", StringType(), False),
])

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL_DOC = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_REL_PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
BUILTIN_DATE_FORMAT_IDS = {
    14, 15, 16, 17, 18, 19, 20, 21, 22, 27, 28, 29, 30, 31, 32, 33,
    34, 35, 36, 45, 46, 47, 50, 51, 52, 53, 54, 55, 56, 57, 58,
}


def excel_column_index(cell_reference: str) -> int:
    letters = re.match(r"[A-Z]+", cell_reference or "", re.I)
    if not letters:
        raise ValueError(f"Invalid Excel cell reference: {cell_reference!r}")
    result = 0
    for char in letters.group(0).upper():
        result = result * 26 + ord(char) - ord("A") + 1
    return result - 1


def _xlsx_shared_strings(archive):
    try:
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    values = []
    for item in root.findall(f"{{{NS_MAIN}}}si"):
        # Include rich-text runs in their original order.
        values.append("".join(
            node.text or "" for node in item.iter(f"{{{NS_MAIN}}}t")
        ))
    return values


def _xlsx_date_style_ids(archive):
    try:
        root = ET.fromstring(archive.read("xl/styles.xml"))
    except KeyError:
        return set()

    custom_formats = {}
    num_fmts = root.find(f"{{{NS_MAIN}}}numFmts")
    if num_fmts is not None:
        for item in num_fmts.findall(f"{{{NS_MAIN}}}numFmt"):
            custom_formats[int(item.attrib["numFmtId"])] = item.attrib.get(
                "formatCode", ""
            )

    xfs = root.find(f"{{{NS_MAIN}}}cellXfs")
    if xfs is None:
        return set()

    date_ids = set()
    for style_index, xf in enumerate(xfs):
        num_fmt_id = int(xf.attrib.get("numFmtId", "0"))
        if num_fmt_id in BUILTIN_DATE_FORMAT_IDS:
            date_ids.add(style_index)
            continue

        fmt = custom_formats.get(num_fmt_id, "")
        # Remove quoted literals, escaped chars and bracket sections before
        # looking for Excel date/time tokens.
        cleaned = re.sub(r'"[^"]*"', "", fmt)
        cleaned = re.sub(r"\[[^\]]*\]", "", cleaned)
        cleaned = re.sub(r"\\.", "", cleaned)
        if re.search(r"[dy]", cleaned, re.I):
            date_ids.add(style_index)
        elif re.search(r"m", cleaned, re.I) and re.search(r"[hs]", cleaned, re.I):
            date_ids.add(style_index)
    return date_ids


def _xlsx_sheet_path(archive):
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    sheets = workbook.find(f"{{{NS_MAIN}}}sheets")
    if sheets is None or not list(sheets):
        raise ValueError("Excel workbook has no worksheets")

    first_sheet = list(sheets)[0]
    relationship_id = first_sheet.attrib.get(f"{{{NS_REL_DOC}}}id")
    if not relationship_id:
        raise ValueError("Excel worksheet relationship ID is missing")

    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    target = None
    for relation in rels.findall(f"{{{NS_REL_PKG}}}Relationship"):
        if relation.attrib.get("Id") == relationship_id:
            target = relation.attrib.get("Target")
            break
    if not target:
        raise ValueError("Cannot resolve first worksheet XML path")

    if target.startswith("/"):
        return target.lstrip("/")
    return posixpath.normpath(posixpath.join("xl", target))


def _xlsx_cell_value(cell, shared_strings, date_style_ids, date1904):
    cell_type = cell.attrib.get("t")
    value_node = cell.find(f"{{{NS_MAIN}}}v")

    if cell_type == "inlineStr":
        inline = cell.find(f"{{{NS_MAIN}}}is")
        if inline is None:
            return ""
        return "".join(
            node.text or "" for node in inline.iter(f"{{{NS_MAIN}}}t")
        )

    if value_node is None or value_node.text is None:
        return None

    value = value_node.text
    if cell_type == "s":
        return shared_strings[int(value)]
    if cell_type in {"str", "e"}:
        return value
    if cell_type == "b":
        return value == "1"

    try:
        number = float(value)
    except ValueError:
        return value

    style_index = int(cell.attrib.get("s", "0"))
    if style_index in date_style_ids:
        epoch = datetime(1904, 1, 1) if date1904 else datetime(1899, 12, 30)
        converted = epoch + timedelta(days=number)
        if converted.time() == datetime.min.time():
            return converted.date().isoformat()
        return converted.isoformat()

    if number.is_integer():
        return int(number)
    return number


def excel_records(raw: bytes):
    """Read the first worksheet; preserve the project's row-1 title/row-2 header convention."""
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        shared_strings = _xlsx_shared_strings(archive)
        date_style_ids = _xlsx_date_style_ids(archive)
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        properties = workbook.find(f"{{{NS_MAIN}}}workbookPr")
        date1904 = (
            properties is not None
            and properties.attrib.get("date1904", "0").lower() in {"1", "true"}
        )
        sheet_path = _xlsx_sheet_path(archive)
        sheet_root = ET.fromstring(archive.read(sheet_path))

        rows = sheet_root.findall(f".//{{{NS_MAIN}}}sheetData/{{{NS_MAIN}}}row")
        header_values = None

        for row in rows:
            row_number = int(row.attrib.get("r", "0"))
            cells = {}
            max_index = -1
            for cell in row.findall(f"{{{NS_MAIN}}}c"):
                index = excel_column_index(cell.attrib.get("r", ""))
                cells[index] = _xlsx_cell_value(
                    cell, shared_strings, date_style_ids, date1904
                )
                max_index = max(max_index, index)

            values = [
                cells.get(index) for index in range(max_index + 1)
            ]

            if row_number == 2:
                header_values = values
                break

        if not header_values:
            raise ValueError("Expected Excel column headers in row 2")

        headers = [
            str(value).strip() if value is not None else f"column_{i}"
            for i, value in enumerate(header_values, start=1)
        ]
        if len(set(headers)) != len(headers):
            raise ValueError("Duplicate Excel column headers")

        for row in rows:
            row_number = int(row.attrib.get("r", "0"))
            if row_number < 3:
                continue

            cells = {}
            max_index = -1
            for cell in row.findall(f"{{{NS_MAIN}}}c"):
                index = excel_column_index(cell.attrib.get("r", ""))
                cells[index] = _xlsx_cell_value(
                    cell, shared_strings, date_style_ids, date1904
                )
                max_index = max(max_index, index)

            if max_index < 0:
                continue

            values = [cells.get(index) for index in range(max_index + 1)]
            if not any(value is not None for value in values):
                continue

            record = {
                headers[index]: values[index]
                for index in range(min(len(headers), len(values)))
            }
            yield row_number, record


def parse_rate(value) -> Decimal:
    text = str(value).strip()
    text = re.sub(r"\s*(?:VNĐ|VND)\s*$", "", text, flags=re.I).strip()
    text = text.replace(" ", "")

    if "," in text and "." in text:
        if re.fullmatch(r"\d{1,3}(?:,\d{3})+\.\d+", text):
            text = text.replace(",", "")
        elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+,\d+", text):
            text = text.replace(".", "").replace(",", ".")
        else:
            raise ValueError(f"Ambiguous exchange-rate number: {value!r}")
    elif "," in text:
        if re.fullmatch(r"\d{1,3}(?:,\d{3})+", text):
            text = text.replace(",", "")
        elif re.fullmatch(r"\d+,\d+", text):
            text = text.replace(",", ".")
        else:
            raise ValueError(f"Invalid exchange-rate number: {value!r}")

    try:
        rate = Decimal(text)
    except (InvalidOperation, ValueError, AttributeError) as exc:
        raise ValueError(f"Invalid exchange rate: {value!r}") from exc

    if not rate.is_finite() or rate <= 0:
        raise ValueError(f"Exchange rate must be positive: {value!r}")
    return rate


def parse_googlefinance_date(value) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("GoogleFinance row is missing date")

    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass

    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported GoogleFinance date: {value!r}")


def googlefinance_records(raw: bytes, object_key: str):
    """Yield (legacy_source_code, date, Date/Close record) from Apps Script JSON."""
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid GoogleFinance JSON: {object_key}") from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise ValueError(
            f"Expected Apps Script JSON envelope with data object: {object_key}"
        )

    mapping = {
        "USD_VND": "usd_vnd_csv",
        "EUR_VND": "eur_vnd_csv",
    }
    yielded = 0

    for pair_name, legacy_source in mapping.items():
        if pair_name not in payload["data"]:
            raise ValueError(f"Missing data.{pair_name} array: {object_key}")
        observations = payload["data"][pair_name]
        if not isinstance(observations, list):
            raise ValueError(f"Expected data.{pair_name} array: {object_key}")

        parsed = []
        for item in observations:
            if not isinstance(item, dict):
                raise ValueError(f"Invalid row in data.{pair_name}: {object_key}")

            raw_date = item.get("date", item.get("Date"))
            raw_close = item.get("close", item.get("Close"))
            if raw_date in (None, "") or raw_close in (None, ""):
                raise ValueError(f"Missing date/close in data.{pair_name}: {object_key}")

            day = parse_googlefinance_date(raw_date)
            rate = parse_rate(raw_close)
            parsed.append((
                day,
                {
                    "Date": day.strftime("%d/%m/%Y 00:00:00"),
                    "Close": format(rate.normalize(), "f"),
                },
            ))

        for day, record in sorted(parsed, key=lambda item: item[0]):
            yielded += 1
            yield legacy_source, day, record

    # An older API batch may have been uploaded successfully while both
    # GoogleFinance arrays were empty. Let the caller report and skip that
    # object so it cannot block valid historical CSV/XLSX or later JSON batches.


def source_from_key(object_key: str) -> str:
    source = next(
        (
            part.split("=", 1)[1]
            for part in object_key.split("/")
            if part.startswith("source=")
        ),
        None,
    )
    if not source:
        raise ValueError(f"MinIO object has no source= path segment: {object_key}")
    return source


def build_bronze_rows(minio: MinioDirect):
    """Read all raw CSV/XLSX/JSON objects in the FX domain."""
    keys = sorted(
        key for key in minio.list_keys(PREFIX)
        if "/raw/" in key.lower()
        and key.lower().endswith((".csv", ".xlsx", ".json"))
    )
    if not keys:
        raise RuntimeError(f"No supported raw FX objects found under {PREFIX}")

    object_sources = {source_from_key(key) for key in keys}
    unsupported = object_sources - SUPPORTED_MINIO_SOURCES
    if unsupported:
        raise RuntimeError(f"Unsupported FX MinIO sources: {sorted(unsupported)}")

    rows = []
    counts = Counter()
    counts_by_type = Counter()
    empty_googlefinance_objects = []

    for object_key in keys:
        raw = minio.read(object_key)
        digest = hashlib.sha256(raw).hexdigest()
        object_source = source_from_key(object_key)

        if object_source == "googlefinance_sheet":
            record_type = "json"
            object_rows = 0
            iterator = (
                (position, legacy_source, record)
                for position, (legacy_source, _day, record)
                in enumerate(googlefinance_records(raw, object_key), start=1)
            )

            for position, legacy_source, record in iterator:
                rows.append((
                    record_id_for(object_key, position),
                    object_key,
                    position,
                    "exchange_rate",
                    legacy_source,
                    record_type,
                    digest,
                    json_record(record),
                ))
                counts[legacy_source] += 1
                counts_by_type[(legacy_source, record_type)] += 1
                object_rows += 1

            if object_rows == 0:
                empty_googlefinance_objects.append(object_key)
                print(
                    "WARNING_EMPTY_GOOGLEFINANCE_OBJECT_SKIPPED:",
                    object_key,
                    flush=True,
                )
            continue

        if object_key.lower().endswith(".xlsx"):
            record_type = "excel"
            iterator = (
                (position, object_source, record)
                for position, record in excel_records(raw)
            )
        elif object_key.lower().endswith(".csv"):
            record_type = "csv"
            iterator = (
                (position, object_source, record)
                for position, record in enumerate(
                    csv_records(object_key, raw), start=1
                )
            )
        else:
            raise ValueError(
                f"JSON is only supported for source=googlefinance_sheet: {object_key}"
            )

        object_rows = 0
        for position, source, record in iterator:
            if not isinstance(record, dict):
                raise ValueError(f"Invalid record in {object_key}")
            rows.append((
                record_id_for(object_key, position),
                object_key,
                position,
                "exchange_rate",
                source,
                record_type,
                digest,
                json_record(record),
            ))
            counts[source] += 1
            counts_by_type[(source, record_type)] += 1
            object_rows += 1

        if object_rows == 0:
            raise RuntimeError(f"Empty source file: {object_key}")

    if not rows:
        raise RuntimeError("No exchange-rate records found after validation")

    return rows, counts, counts_by_type, len(keys), empty_googlefinance_objects


def main():
    parser = argparse.ArgumentParser(
        description="Ingest all retained FX raw objects into Bronze Iceberg."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read MinIO and validate rows without writing Iceberg.",
    )
    args = parser.parse_args()

    minio = MinioDirect()
    rows, counts, counts_by_type, object_count, empty_googlefinance_objects = (
        build_bronze_rows(minio)
    )

    print("SOURCE_OBJECTS:", object_count, flush=True)
    print("EMPTY_GOOGLEFINANCE_OBJECTS_SKIPPED:", len(empty_googlefinance_objects), flush=True)
    print("BRONZE_ROWS_BY_SOURCE:", dict(counts), flush=True)
    print("BRONZE_ROWS_BY_SOURCE_AND_TYPE:", dict(counts_by_type), flush=True)
    print("TOTAL_CANDIDATE_BRONZE_ROWS:", len(rows), flush=True)

    if args.dry_run:
        print("BRONZE_EXCHANGE_RATE_DRY_RUN_PASSED", flush=True)
        print("NO_ICEBERG_WRITE", flush=True)
        return

    spark = spark_session("TLCN-Bronze-Exchange-Rate")
    try:
        if spark.catalog.tableExists(TABLE):
            from pyspark.sql import functions as F

            separate_model_rows = (
                spark.table(TABLE)
                .filter(F.col("source") == "googlefinance_sheet")
                .limit(1)
                .count()
            )
            if separate_model_rows:
                raise RuntimeError(
                    "Bronze Iceberg already contains source=googlefinance_sheet "
                    "from a separate-source model. Reconcile it before writing "
                    "legacy-mapped GoogleFinance rows."
                )

        merge_into_bronze(
            spark,
            TABLE,
            SCHEMA,
            rows,
            "exchange_rate_direct",
            "BRONZE_EXCHANGE_RATE",
        )

        spark.sql(f"""
            SELECT source, record_type,
                   COUNT(*) AS records,
                   COUNT(DISTINCT object_key) AS source_objects
            FROM {TABLE}
            GROUP BY source, record_type
            ORDER BY source, record_type
        """).show(truncate=False)

        print("BRONZE_EXCHANGE_RATE_PASSED", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
