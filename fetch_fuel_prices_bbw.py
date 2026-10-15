"""
==============================================================
Fetch Vietnam Fuel Prices from BBW API (Bronze Layer)
Project : Coffee Robusta Lakehouse
Author  : Thanh Truong
==============================================================
"""

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import json

# ==============================================================
# CONFIG
# ==============================================================

BASE_URL = "https://bbw.vn/api/proxy/commodity-spot-prices"

START_YEAR = 2000
END_YEAR = datetime.today().year

LIMIT = 100
SLEEP_TIME = 0.3        # tránh spam API

OUTPUT_DIR = Path("raw_data/fuel_bbw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================
# FETCH API
# ==============================================================


def fetch_year(year: int):
    """Lấy toàn bộ dữ liệu của một năm"""

    from_date = f"{year}-01-01"
    to_date = f"{year}-12-31"

    page = 1
    all_records = []

    print(f"\n========== YEAR {year} ==========")

    while True:

        params = {
            "product": "FUEL",
            "from": from_date,
            "to": to_date,
            "sort": "oldest",
            "limit": LIMIT,
            "page": page,
        }

        r = requests.get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()

        payload = r.json()
        data = payload.get("data", [])

        if len(data) == 0:
            break

        print(f"Page {page:02d} | {len(data)} records")

        all_records.extend(data)

        if len(data) < LIMIT:
            break

        page += 1
        time.sleep(SLEEP_TIME)

    # lưu raw json từng năm
    raw_path = OUTPUT_DIR / f"fuel_{year}.json"

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"Saved raw JSON -> {raw_path.name}")

    return all_records


# ==============================================================
# NORMALIZE JSON
# ==============================================================


def normalize(records):

    rows = []

    for item in records:

        meta = item.get("meta", {})
        source = item.get("source", {})

        rows.append(
            {
                "quote_date": item.get("quote_date_vn"),
                "quoted_at": item.get("quoted_at"),
                "updated_at": item.get("updated_at"),

                "product": item.get("product"),
                "region": item.get("region"),
                "series_code": item.get("series_code"),

                "fuel_name": meta.get("fuel_name"),
                "zone": meta.get("zone"),
                "provider": meta.get("provider"),

                "price_vnd_liter": item.get("price"),
                "currency": item.get("currency"),
                "unit": item.get("unit"),

                "source_code": source.get("code"),
                "source_name": source.get("name"),
            }
        )

    df = pd.DataFrame(rows)

    if len(df) == 0:
        return df

    df["quote_date"] = pd.to_datetime(df["quote_date"])
    df["quoted_at"] = pd.to_datetime(df["quoted_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])

    df["price_vnd_liter"] = pd.to_numeric(df["price_vnd_liter"])

    return df


# ==============================================================
# MAIN
# ==============================================================


def main():

    all_records = []

    for year in range(START_YEAR, END_YEAR + 1):
        try:
            records = fetch_year(year)
            all_records.extend(records)
        except Exception as e:
            print(f"Failed year {year}: {e}")

    print("\n======================================")
    print(f"TOTAL RAW RECORDS : {len(all_records)}")
    print("======================================")

    df = normalize(all_records)

    # loại duplicate
    df = (
        df.sort_values("updated_at")
          .drop_duplicates(
              subset=["quote_date", "series_code", "zone"],
              keep="last",
          )
          .reset_index(drop=True)
    )

    csv_path = OUTPUT_DIR / "fuel_prices_bbw_2000_present.csv"
    parquet_path = OUTPUT_DIR / "fuel_prices_bbw_2000_present.parquet"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_parquet(parquet_path, index=False)

    print("\nSaved CSV      :", csv_path)
    print("Saved Parquet  :", parquet_path)

    print("\n========== SUMMARY ==========")
    print(df.info())
    print()

    print("Date range")
    print(df["quote_date"].min(), "->", df["quote_date"].max())

    print("\nFuel types")
    print(df["fuel_name"].value_counts())

    print("\nZones")
    print(df["zone"].value_counts())

    print("\nSample")
    print(df.head())


if __name__ == "__main__":
    main()