"""Silver exchange rate: validate and normalize all Bronze observations.

The job is incremental and append-only:
- it accepts any non-zero number of valid Bronze observations;
- identical daily duplicates are collapsed by business key;
- historical CSV wins over GoogleFinance JSON for the same business key;
- conflicting values within the same file type stop the write;
- existing Silver rows are never deleted or overwritten.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from minio_direct import spark_session

SOURCE = "lakehouse.bronze.exchange_rate_raw"
TARGET = "lakehouse.silver.exchange_rate_observations"

IMF_SERIES = {
    "VNM.XDC_USD.PA_RT.M": ("USD/VND", "PERIOD_AVERAGE"),
    "VNM.XDC_USD.EOP_RT.M": ("USD/VND", "END_OF_PERIOD"),
    "VNM.XDC_EUR.PA_RT.M": ("EUR/VND", "PERIOD_AVERAGE"),
    "VNM.XDC_EUR.EOP_RT.M": ("EUR/VND", "END_OF_PERIOD"),
}


def parse_vnd(value, vietnamese=False):
    text = str(value or "").strip()
    text = re.sub(r"\s*(?:VNĐ|VND)\s*$", "", text, flags=re.I).strip()
    text = text.replace(" ", "")

    if vietnamese:
        text = text.replace(".", "").replace(",", ".")
    else:
        if "," in text and "." in text:
            if re.fullmatch(r"\d{1,3}(?:,\d{3})+\.\d+", text):
                text = text.replace(",", "")
            elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+,\d+", text):
                text = text.replace(".", "").replace(",", ".")
            else:
                raise ValueError(f"Ambiguous numeric rate: {text}")
        elif "," in text:
            if re.fullmatch(r"\d{1,3}(?:,\d{3})+", text):
                text = text.replace(",", "")
            elif re.fullmatch(r"\d+,\d+", text):
                text = text.replace(",", ".")
            else:
                raise ValueError(f"Invalid numeric rate: {text}")
        elif not re.fullmatch(r"\d+(?:\.\d+)?", text):
            raise ValueError(f"Invalid numeric rate: {text}")

    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid numeric rate: {text}") from exc

    if not number.is_finite() or number <= 0:
        raise ValueError(f"Invalid positive rate: {text}")
    return number


def observation_id(record_id, period=""):
    return hashlib.sha256(
        (record_id + "|" + period).encode("utf-8")
    ).hexdigest()


def googlefinance_observation_id(source, day):
    """Stable ID for a GoogleFinance daily observation across API snapshots."""
    natural_key = f"{source}|{day.isoformat()}|DAILY|CLOSE"
    return hashlib.sha256(natural_key.encode("utf-8")).hexdigest()


def parse_daily_date(value):
    text = str(value or "").strip()
    if not text:
        raise ValueError("Missing daily observation date")

    for fmt in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            continue

    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(f"Unsupported daily observation date: {value!r}") from exc


def normalize(row):
    raw = json.loads(row.raw_record_json)
    source = row.source
    result = []

    base = {
        "source": source,
        "bronze_record_id": row.record_id,
        "object_key": row.object_key,
        # Kept as an in-memory provenance field for deterministic source
        # precedence. It is not part of the persisted Silver schema.
        "record_type": getattr(row, "record_type", ""),
    }

    if source in ("usd_vnd_csv", "eur_vnd_csv"):
        pair = "USD/VND" if source.startswith("usd") else "EUR/VND"
        day = parse_daily_date(raw.get("Date"))
        rate = parse_vnd(raw.get("Close"))

        # GoogleFinance is stored as record_type=json and mapped to the same
        # legacy source code. A natural-key ID prevents each full API snapshot
        # from creating a new Silver row for an already-seen date.
        if getattr(row, "record_type", None) == "json":
            current_id = googlefinance_observation_id(source, day)
        else:
            current_id = observation_id(row.record_id)

        result.append({
            **base,
            "observation_id": current_id,
            "currency_pair": pair,
            "observation_date": day.isoformat(),
            "frequency": "DAILY",
            "rate_type": "CLOSE",
            "rate_vnd": str(rate),
        })

    elif source in ("usd_vnd_excel", "eur_vnd_excel"):
        pair = "USD/VND" if source.startswith("usd") else "EUR/VND"
        expected = pair.split("/")[0]
        currency_code = str(raw.get("column_1", "")).strip().upper()

        if currency_code != expected:
            raise ValueError(
                f"Excel currency code mismatch: expected={expected}, "
                f"actual={currency_code!r}"
            )

        raw_day = raw.get("Ngày hiệu lực")
        if raw_day in (None, ""):
            raise ValueError("Excel row is missing Ngày hiệu lực")
        day = parse_daily_date(raw_day)
        rate = parse_vnd(raw.get("Tỷ giá"), vietnamese=True)

        result.append({
            **base,
            "observation_id": observation_id(row.record_id),
            "currency_pair": pair,
            "observation_date": day.isoformat(),
            "frequency": "DAILY",
            "rate_type": "PUBLISHED",
            "rate_vnd": str(rate),
        })

    elif source == "imf_exchange_rate":
        series = raw.get("SERIES_CODE", "")
        if series not in IMF_SERIES:
            return []

        pair, rate_type = IMF_SERIES[series]
        for period, value in raw.items():
            if not re.fullmatch(r"\d{4}-M(0[1-9]|1[0-2])", period):
                continue
            if value is None or str(value).strip() == "":
                continue

            day = datetime.strptime(
                period.replace("-M", "-"),
                "%Y-%m",
            ).date()
            rate = parse_vnd(value)

            result.append({
                **base,
                "observation_id": observation_id(row.record_id, period),
                "currency_pair": pair,
                "observation_date": day.isoformat(),
                "frequency": "MONTHLY",
                "rate_type": rate_type,
                "rate_vnd": str(rate),
            })

    else:
        raise ValueError(f"Unsupported source: {source}")

    return result


def business_key(record):
    return (
        record["source"],
        record["currency_pair"],
        record["observation_date"],
        record["frequency"],
        record["rate_type"],
    )


def deduplicate_observations(observations):
    """Deduplicate by business key, preferring historical CSV over API JSON.

    CSV is the established historical source. GoogleFinance JSON is an
    enrichment source: when both represent the same business key, CSV wins.
    Conflicts between two records of the same type remain hard errors.
    """
    priority = {"csv": 0, "json": 1}
    by_key = {}
    duplicate_count = 0
    resolved_conflicts = []
    conflicts = []

    for record in observations:
        key = business_key(record)
        previous = by_key.get(key)

        if previous is None:
            by_key[key] = record
            continue

        previous_rate = Decimal(previous["rate_vnd"])
        incoming_rate = Decimal(record["rate_vnd"])
        previous_type = str(previous.get("record_type", "")).lower()
        incoming_type = str(record.get("record_type", "")).lower()

        if previous_rate == incoming_rate:
            duplicate_count += 1
            # Keep the preferred lineage even if input ordering changes.
            if priority.get(incoming_type, 99) < priority.get(previous_type, 99):
                by_key[key] = record
            continue

        # Resolve only a CSV-vs-JSON disagreement. Do not silently resolve
        # conflicting CSV rows, conflicting JSON rows, or other source types.
        if {previous_type, incoming_type} == {"csv", "json"}:
            winner = (
                previous
                if priority.get(previous_type, 99) < priority.get(incoming_type, 99)
                else record
            )
            loser = record if winner is previous else previous
            by_key[key] = winner
            resolved_conflicts.append((
                key,
                winner["rate_vnd"],
                loser["rate_vnd"],
                winner["object_key"],
                loser["object_key"],
                "csv_preferred_over_json",
            ))
            continue

        conflicts.append((
            key,
            previous["rate_vnd"],
            record["rate_vnd"],
            previous["object_key"],
            record["object_key"],
            previous_type,
            incoming_type,
        ))

    return (
        list(by_key.values()),
        duplicate_count,
        resolved_conflicts,
        conflicts,
    )


def write_silver(spark, observations):
    from pyspark.sql import functions as F

    if not observations:
        raise RuntimeError("No Silver observations to write")

    df = spark.createDataFrame(observations).select(
        F.col("observation_id"),
        F.col("currency_pair"),
        F.to_date("observation_date").alias("observation_date"),
        F.col("frequency"),
        F.col("rate_type"),
        F.col("rate_vnd").cast("decimal(28,12)").alias("rate_vnd"),
        F.col("source"),
        F.col("bronze_record_id"),
        F.col("object_key"),
    ).cache()

    try:
        count = df.count()
        unique_incoming = df.select("observation_id").distinct().count()

        if count == 0:
            raise RuntimeError("No Silver observations to write")
        if unique_incoming != count:
            raise RuntimeError(
                f"Incoming Silver observation IDs are not unique: "
                f"rows={count}, unique={unique_incoming}"
            )

        if df.filter(
            "observation_date IS NULL OR rate_vnd IS NULL OR rate_vnd <= 0"
        ).limit(1).count():
            raise RuntimeError("Invalid typed Silver observations")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET} (
                observation_id STRING,
                currency_pair STRING,
                observation_date DATE,
                frequency STRING,
                rate_type STRING,
                rate_vnd DECIMAL(28,12),
                source STRING,
                bronze_record_id STRING,
                object_key STRING
            ) USING iceberg
        """)

        existing = spark.table(TARGET)
        if set(existing.columns) != set(df.columns):
            raise RuntimeError("Existing Silver table schema mismatch")

        business_columns = [
            "source",
            "currency_pair",
            "observation_date",
            "frequency",
            "rate_type",
        ]

        existing_count = existing.count()
        existing_unique = existing.select("observation_id").distinct().count()
        if existing_count != existing_unique:
            raise RuntimeError(
                "Existing Silver table contains duplicate observation_id values; "
                "reconcile the table before incremental write"
            )

        # Preserve the old Silver row when the same daily observation arrives
        # again from GoogleFinance under a different Bronze record_id.
        existing_duplicate_business_keys = (
            existing.groupBy(*business_columns)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if existing_duplicate_business_keys:
            raise RuntimeError(
                "Existing Silver table already has duplicate business keys; "
                "reconcile before incremental write"
            )

        # If the exact same observation_id is already stored, all its values
        # and lineage must still match. Never overwrite changed old rows.
        id_comparison = df.alias("incoming").join(
            existing.alias("stored"),
            on="observation_id",
            how="inner",
        )
        id_mismatch = None
        for name in df.columns:
            if name == "observation_id":
                continue
            difference = ~F.col(
                f"incoming.{name}"
            ).eqNullSafe(F.col(f"stored.{name}"))
            id_mismatch = (
                difference if id_mismatch is None else id_mismatch | difference
            )
        if id_comparison.filter(id_mismatch).limit(1).count():
            raise RuntimeError(
                "Existing Silver observation_id has changed values; refusing overwrite"
            )

        # Compare business keys separately from observation_id. This prevents
        # an old CSV observation and a new GoogleFinance snapshot of the same
        # date from becoming two Silver rows just because their Bronze IDs differ.
        business_comparison = df.alias("incoming").join(
            existing.alias("stored"),
            on=business_columns,
            how="inner",
        )
        if business_comparison.filter(
            ~F.col("incoming.rate_vnd").eqNullSafe(F.col("stored.rate_vnd"))
        ).limit(1).count():
            raise RuntimeError(
                "Same Silver business key has a different rate in Bronze and "
                "existing Silver; refusing to overwrite historical data"
            )

        new_df = df.join(
            existing.select(*business_columns),
            on=business_columns,
            how="left_anti",
        ).cache()
        try:
            new_count = new_df.count()
            print("SILVER_NEW_RECORDS:", new_count, flush=True)

            if new_count:
                new_df.createOrReplaceTempView("exchange_rate_silver_stage")
                spark.sql(f"""
                    MERGE INTO {TARGET} AS t
                    USING exchange_rate_silver_stage AS s
                    ON t.observation_id = s.observation_id
                    WHEN NOT MATCHED THEN INSERT *
                """)
        finally:
            new_df.unpersist()

        final = spark.table(TARGET)
        total = final.count()
        unique = final.select("observation_id").distinct().count()
        final_duplicate_business_keys = (
            final.groupBy(*business_columns)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        matched = (
            final.select(*business_columns)
            .join(df.select(*business_columns), on=business_columns, how="inner")
            .count()
        )

        print("SILVER_INCOMING_ROWS:", count, flush=True)
        print("SILVER_EXCHANGE_RATE_TOTAL:", total, flush=True)
        print("SILVER_UNIQUE_IDS:", unique, flush=True)
        print("SILVER_INCOMING_BUSINESS_KEYS_PRESENT:", matched, flush=True)

        if (
            total < count
            or unique != total
            or final_duplicate_business_keys
            or matched != count
        ):
            raise RuntimeError("Silver append-only verification failed")

        final.groupBy(
            "currency_pair", "frequency", "rate_type"
        ).count().orderBy(
            "currency_pair", "frequency", "rate_type"
        ).show(truncate=False)

        print("SILVER_EXCHANGE_RATE_ICEBERG_PASSED", flush=True)
    finally:
        df.unpersist()


def main():
    spark = spark_session("TLCN-Silver-Exchange-Rate")
    try:
        bronze = (
            spark.table(SOURCE)
            .select(
                "record_id",
                "object_key",
                "source",
                "record_type",
                "raw_record_json",
            )
            # CSV sorts before JSON so the pre-existing local-upload observation
            # remains the representative row for overlapping daily dates.
            .orderBy("source", "record_type", "object_key", "record_id")
        )

        rows = bronze.collect()
        print("BRONZE_RECORDS:", len(rows), flush=True)

        if not rows:
            raise RuntimeError("No Bronze exchange-rate rows found")

        observations = []
        rejected = []
        counts = Counter()

        for row in rows:
            try:
                converted = normalize(row)
                observations.extend(converted)
                counts[row.source] += len(converted)
            except (ValueError, KeyError, InvalidOperation, TypeError) as exc:
                rejected.append((row.source, row.record_id, str(exc)))

        print("=== SILVER NORMALIZATION ===", flush=True)
        for source, count in sorted(counts.items()):
            print(f"{source}: {count}", flush=True)

        print("NORMALIZED_OBSERVATIONS_BEFORE_DEDUP:", len(observations), flush=True)
        print("REJECTED_RECORDS:", len(rejected), flush=True)
        for item in rejected[:10]:
            print("REJECTED_SAMPLE:", item, flush=True)

        (
            observations,
            duplicate_count,
            resolved_conflicts,
            conflicts,
        ) = deduplicate_observations(observations)
        ids = [record["observation_id"] for record in observations]

        print("IDENTICAL_BUSINESS_DUPLICATES_COLLAPSED:", duplicate_count, flush=True)
        print(
            "CSV_PREFERRED_CONFLICTS_RESOLVED:",
            len(resolved_conflicts),
            flush=True,
        )
        print("UNRESOLVED_CONFLICTING_BUSINESS_KEYS:", len(conflicts), flush=True)
        print("NORMALIZED_OBSERVATIONS:", len(observations), flush=True)
        print("UNIQUE_OBSERVATION_IDS:", len(set(ids)), flush=True)

        for item in resolved_conflicts[:10]:
            print("CSV_PREFERRED_CONFLICT_RESOLVED:", item, flush=True)

        for item in conflicts[:10]:
            print("UNRESOLVED_CONFLICT_SAMPLE:", item, flush=True)

        if len(ids) != len(set(ids)):
            raise RuntimeError("Duplicate observation IDs after business-key deduplication")
        if not observations:
            raise RuntimeError("No valid Silver observations")

        print("=== OBSERVATIONS BY PAIR AND FREQUENCY ===", flush=True)
        grouped = Counter(
            (
                record["currency_pair"],
                record["frequency"],
                record["rate_type"],
            )
            for record in observations
        )
        for key, count in sorted(grouped.items()):
            print(f"{key}: {count}", flush=True)

        if rejected or conflicts:
            raise RuntimeError(
                "Silver validation failed. Review rejected records and "
                "conflicting business keys before continuing."
            )

        if "--write" in sys.argv[1:]:
            write_silver(spark, observations)
        else:
            print("SILVER_EXCHANGE_RATE_DRY_RUN_PASSED", flush=True)
            print("NO_SILVER_TABLE_WRITTEN", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
