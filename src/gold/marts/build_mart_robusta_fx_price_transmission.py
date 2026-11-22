#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Build DM15: Robusta FX & Price Transmission.

Sources:
    lakehouse.gold.mart_futures_domestic_spread
    lakehouse.gold.fact_exchange_rate
    lakehouse.gold.fact_futures_price
    lakehouse.gold.dim_source

Target:
    lakehouse.gold.mart_robusta_fx_price_transmission

Grain:
    market_date + province + price_series_key

Analysis:
    - Convert London Robusta Futures from USD/tonne to VND/kg using USD/VND.
    - Compare converted futures with domestic Robusta prices (indicative basis).
    - Measure FX contribution to the VND-equivalent futures price.
    - Calculate daily returns and rolling 30-observation correlations/betas.

Important:
    FUTURES_UNIT is explicitly assumed to be USD/TONNE based on the London
    Robusta benchmark convention and observed price scale. Verify this against
    the original CSV/data dictionary before using the converted metrics in a
    formal report. This script prints the assumption in its run header.

FX alignment:
    Uses the latest USD/VND DAILY CLOSE observation on or before market_date.
    It never uses a future FX observation. A maximum 4-calendar-day age allows
    weekends/short market holidays; missing/staler FX is marked for review.

Default mode is DRY RUN. Pass --write to create the Iceberg table.
The script refuses to overwrite an existing target table.
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BASE_TABLE = "lakehouse.gold.mart_futures_domestic_spread"
FX_TABLE = "lakehouse.gold.fact_exchange_rate"
FUTURES_FACT_TABLE = "lakehouse.gold.fact_futures_price"
SOURCE_DIM_TABLE = "lakehouse.gold.dim_source"
TARGET_TABLE = "lakehouse.gold.mart_robusta_fx_price_transmission"

FX_PAIR = "USD/VND"
FX_FREQUENCY = "DAILY"
FX_RATE_TYPE = "CLOSE"
FX_SOURCE = "usd_vnd_csv"
MAX_FX_AGE_DAYS = 4

EXPECTED_FUTURES_SOURCE_KEY = 1013198516018842028
EXPECTED_FUTURES_SOURCE_CODE = "historical_csv"
FUTURES_UNIT = "USD/TONNE"
FUTURES_TO_VND_KG_DIVISOR = 1000.0


def require_columns(df, required, table_name):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise RuntimeError(
            table_name + " is missing required columns: " + ", ".join(missing)
        )


def safe_pct(numerator, denominator):
    return F.when(
        numerator.isNotNull()
        & denominator.isNotNull()
        & (denominator != F.lit(0)),
        (numerator / denominator - F.lit(1.0)) * F.lit(100.0),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build Robusta FX and price-transmission Gold Data Mart."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Create the Iceberg target table. Without this flag, run validation only.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("TLCN-Build-Mart-Robusta-FX-Price-Transmission")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("=" * 78)
        print("DM15 - ROBUSTA FX & PRICE TRANSMISSION")
        print("BASE:", BASE_TABLE)
        print("FX:", FX_TABLE)
        print("TARGET:", TARGET_TABLE)
        print("GRAIN: market_date + province + price_series_key")
        print("FX POLICY: latest USD/VND DAILY CLOSE on/before market_date")
        print("MAX FX AGE DAYS:", MAX_FX_AGE_DAYS)
        print("FUTURES UNIT ASSUMPTION:", FUTURES_UNIT)
        print("CONVERSION: USD/tonne * VND/USD / 1000 = VND/kg")
        print("MODE:", "WRITE" if args.write else "DRY RUN")
        print("=" * 78)

        # ------------------------------------------------------------------
        # 1. Validate the Futures source lineage behind the unit assumption.
        # ------------------------------------------------------------------
        futures_fact = spark.table(FUTURES_FACT_TABLE)
        require_columns(
            futures_fact,
            ["source_key", "last_price", "quality_status"],
            FUTURES_FACT_TABLE,
        )

        source_dim = spark.table(SOURCE_DIM_TABLE)
        require_columns(source_dim, ["source_key", "source_code"], SOURCE_DIM_TABLE)

        futures_source_profile = (
            futures_fact
            .groupBy("source_key")
            .count()
            .join(source_dim.select("source_key", "source_code"), "source_key", "left")
            .collect()
        )

        source_profile_text = [
            (int(r["source_key"]), r["source_code"], int(r["count"]))
            for r in futures_source_profile
        ]
        print("FUTURES_SOURCE_PROFILE:", source_profile_text)

        if len(source_profile_text) != 1:
            raise RuntimeError(
                "Expected exactly one Futures source for the USD/tonne assumption. "
                "Found multiple source keys; verify units per source before building DM15."
            )

        actual_source_key, actual_source_code, _ = source_profile_text[0]
        if (
            actual_source_key != EXPECTED_FUTURES_SOURCE_KEY
            or actual_source_code != EXPECTED_FUTURES_SOURCE_CODE
        ):
            raise RuntimeError(
                "Futures source lineage changed. Expected source_key "
                + str(EXPECTED_FUTURES_SOURCE_KEY)
                + " / "
                + EXPECTED_FUTURES_SOURCE_CODE
                + ", found "
                + str(actual_source_key)
                + " / "
                + str(actual_source_code)
                + ". Reconfirm the quoted unit before conversion."
            )

        # ------------------------------------------------------------------
        # 2. Read the existing domestic-vs-London mart as the base grain.
        # ------------------------------------------------------------------
        base = spark.table(BASE_TABLE)
        require_columns(
            base,
            [
                "market_date",
                "province",
                "price_series_key",
                "source_family",
                "domestic_price_vnd_kg",
                "domestic_quality_status",
                "domestic_change_pct",
                "futures_avg_last_price",
                "futures_avg_change_pct",
                "futures_observation_count",
                "futures_valid_price_count",
                "futures_ohlc_review_count",
            ],
            BASE_TABLE,
        )

        base = base.select(
            F.col("market_date").cast("date").alias("market_date"),
            "province",
            "price_series_key",
            "source_family",
            "domestic_price_vnd_kg",
            "domestic_quality_status",
            "domestic_change_pct",
            F.col("futures_avg_last_price").alias("futures_price_usd_per_tonne"),
            "futures_avg_change_pct",
            "futures_observation_count",
            "futures_valid_price_count",
            "futures_ohlc_review_count",
        )

        base_rows = base.count()
        if base_rows == 0:
            raise RuntimeError("Base mart contains no rows.")

        base_duplicate_keys = (
            base.groupBy("market_date", "province", "price_series_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        base_null_keys = base.filter(
            F.col("market_date").isNull()
            | F.col("province").isNull()
            | F.col("price_series_key").isNull()
        ).count()

        if base_duplicate_keys or base_null_keys:
            raise RuntimeError(
                "Base mart grain is invalid. duplicate_keys="
                + str(base_duplicate_keys)
                + ", null_key_rows="
                + str(base_null_keys)
            )

        # ------------------------------------------------------------------
        # 3. Select only USD/VND DAILY CLOSE from the designated CSV source.
        # ------------------------------------------------------------------
        fx_source = spark.table(FX_TABLE)
        require_columns(
            fx_source,
            [
                "observation_date",
                "currency_pair",
                "source",
                "frequency",
                "rate_type",
                "rate_vnd",
            ],
            FX_TABLE,
        )

        fx = (
            fx_source
            .filter(F.col("currency_pair") == FX_PAIR)
            .filter(F.col("frequency") == FX_FREQUENCY)
            .filter(F.col("rate_type") == FX_RATE_TYPE)
            .filter(F.col("source") == FX_SOURCE)
            .select(
                F.col("observation_date").cast("date").alias("fx_observation_date"),
                F.col("rate_vnd").cast("double").alias("fx_rate_vnd_per_usd"),
                F.col("currency_pair").alias("fx_currency_pair"),
                F.col("source").alias("fx_source"),
                F.col("rate_type").alias("fx_rate_type"),
            )
        )

        fx_rows = fx.count()
        fx_distinct_dates = fx.select("fx_observation_date").distinct().count()
        fx_invalid_rows = fx.filter(
            F.col("fx_observation_date").isNull()
            | F.col("fx_rate_vnd_per_usd").isNull()
            | (F.col("fx_rate_vnd_per_usd") <= 0)
        ).count()
        fx_duplicate_dates = (
            fx.groupBy("fx_observation_date")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("DM15_BASE_SOURCE_ROWS:", base_rows)
        print("DM15_FX_SOURCE_ROWS:", fx_rows)
        print("DM15_FX_DISTINCT_DATES:", fx_distinct_dates)
        print("DM15_FX_DUPLICATE_DATES:", fx_duplicate_dates)
        print("DM15_FX_INVALID_ROWS:", fx_invalid_rows)

        if fx_rows == 0:
            raise RuntimeError(
                "No USD/VND DAILY CLOSE rows found for source usd_vnd_csv."
            )
        if fx_invalid_rows != 0:
            raise RuntimeError("Invalid USD/VND DAILY CLOSE rows detected.")
        if fx_duplicate_dates != 0:
            raise RuntimeError(
                "USD/VND DAILY CLOSE has more than one row per observation_date."
            )
        if fx_rows != fx_distinct_dates:
            raise RuntimeError(
                "FX row count does not match distinct observation dates."
            )

        fx_window = Window.orderBy("fx_observation_date")
        fx = (
            fx
            .withColumn(
                "fx_previous_rate_vnd_per_usd",
                F.lag("fx_rate_vnd_per_usd").over(fx_window),
            )
            .withColumn(
                "fx_close_change_pct",
                F.when(
                    F.col("fx_previous_rate_vnd_per_usd").isNotNull()
                    & (F.col("fx_previous_rate_vnd_per_usd") > 0),
                    F.round(
                        (
                            F.col("fx_rate_vnd_per_usd")
                            / F.col("fx_previous_rate_vnd_per_usd")
                            - F.lit(1.0)
                        )
                        * F.lit(100.0),
                        6,
                    ),
                ),
            )
        )

        # ------------------------------------------------------------------
        # 4. As-of join. A rate can only be carried backward, never forward.
        #    Keep at most 4 calendar days of age to bridge weekends/holidays.
        # ------------------------------------------------------------------
        b = base.alias("b")
        x = fx.alias("x")
        joined = b.join(
            x,
            (
                F.col("x.fx_observation_date") <= F.col("b.market_date")
            )
            & (
                F.datediff(
                    F.col("b.market_date"), F.col("x.fx_observation_date")
                )
                <= F.lit(MAX_FX_AGE_DAYS)
            ),
            "left",
        )

        pick_latest_fx = (
            Window.partitionBy(
                F.col("b.market_date"),
                F.col("b.province"),
                F.col("b.price_series_key"),
            )
            .orderBy(F.col("x.fx_observation_date").desc_nulls_last())
        )

        aligned = (
            joined
            .select(
                "b.*",
                F.col("x.fx_observation_date").alias("fx_observation_date"),
                F.col("x.fx_rate_vnd_per_usd").alias("fx_rate_vnd_per_usd"),
                F.col("x.fx_previous_rate_vnd_per_usd").alias(
                    "fx_previous_rate_vnd_per_usd"
                ),
                F.col("x.fx_close_change_pct").alias("fx_close_change_pct"),
                F.col("x.fx_currency_pair").alias("fx_currency_pair"),
                F.col("x.fx_source").alias("fx_source"),
                F.col("x.fx_rate_type").alias("fx_rate_type"),
                F.row_number().over(pick_latest_fx).alias("_fx_rank"),
            )
            .filter(F.col("_fx_rank") == 1)
            .drop("_fx_rank")
        )

        # ------------------------------------------------------------------
        # 5. Convert Futures and calculate indicative domestic-vs-global basis.
        # ------------------------------------------------------------------
        aligned = (
            aligned
            .withColumn(
                "fx_age_days",
                F.when(
                    F.col("fx_observation_date").isNotNull(),
                    F.datediff("market_date", "fx_observation_date"),
                ).cast("int"),
            )
            .withColumn(
                "fx_quality_status",
                F.when(F.col("fx_rate_vnd_per_usd").isNull(), F.lit("MISSING"))
                .when(F.col("fx_age_days") == 0, F.lit("CURRENT"))
                .when(
                    F.col("fx_age_days") <= MAX_FX_AGE_DAYS,
                    F.lit("CARRIED_FORWARD"),
                )
                .otherwise(F.lit("STALE")),
            )
            .withColumn(
                "futures_price_vnd_per_kg",
                F.round(
                    F.col("futures_price_usd_per_tonne")
                    * F.col("fx_rate_vnd_per_usd")
                    / F.lit(FUTURES_TO_VND_KG_DIVISOR),
                    2,
                ),
            )
            .withColumn(
                "basis_domestic_minus_futures_vnd_kg",
                F.round(
                    F.col("domestic_price_vnd_kg")
                    - F.col("futures_price_vnd_per_kg"),
                    2,
                ),
            )
            .withColumn(
                "basis_pct_of_futures",
                F.when(
                    F.col("futures_price_vnd_per_kg").isNotNull()
                    & (F.col("futures_price_vnd_per_kg") > 0),
                    F.round(
                        (
                            F.col("domestic_price_vnd_kg")
                            / F.col("futures_price_vnd_per_kg")
                            - F.lit(1.0)
                        )
                        * F.lit(100.0),
                        3,
                    ),
                ),
            )
            .withColumn(
                "fx_effect_on_futures_vnd_kg",
                F.when(
                    F.col("fx_previous_rate_vnd_per_usd").isNotNull(),
                    F.round(
                        F.col("futures_price_usd_per_tonne")
                        * (
                            F.col("fx_rate_vnd_per_usd")
                            - F.col("fx_previous_rate_vnd_per_usd")
                        )
                        / F.lit(FUTURES_TO_VND_KG_DIVISOR),
                        2,
                    ),
                ),
            )
        )

        # ------------------------------------------------------------------
        # 6. Daily returns and rolling 30-observation transmission metrics.
        # ------------------------------------------------------------------
        series_window = Window.partitionBy(
            "province", "price_series_key"
        ).orderBy("market_date")

        aligned = (
            aligned
            .withColumn(
                "_previous_domestic_price",
                F.lag("domestic_price_vnd_kg").over(series_window),
            )
            .withColumn(
                "_previous_futures_vnd_kg",
                F.lag("futures_price_vnd_per_kg").over(series_window),
            )
            .withColumn(
                "domestic_return_pct",
                F.when(
                    F.col("_previous_domestic_price").isNotNull()
                    & (F.col("_previous_domestic_price") > 0),
                    F.round(
                        (
                            F.col("domestic_price_vnd_kg")
                            / F.col("_previous_domestic_price")
                            - F.lit(1.0)
                        )
                        * F.lit(100.0),
                        6,
                    ),
                ),
            )
            .withColumn(
                "futures_vnd_return_pct",
                F.when(
                    F.col("_previous_futures_vnd_kg").isNotNull()
                    & (F.col("_previous_futures_vnd_kg") > 0),
                    F.round(
                        (
                            F.col("futures_price_vnd_per_kg")
                            / F.col("_previous_futures_vnd_kg")
                            - F.lit(1.0)
                        )
                        * F.lit(100.0),
                        6,
                    ),
                ),
            )
            .withColumn(
                "domestic_futures_return_gap_pp",
                F.when(
                    F.col("domestic_return_pct").isNotNull()
                    & F.col("futures_vnd_return_pct").isNotNull(),
                    F.round(
                        F.col("domestic_return_pct")
                        - F.col("futures_vnd_return_pct"),
                        6,
                    ),
                ),
            )
        )

        rolling_window = (
            Window.partitionBy("province", "price_series_key")
            .orderBy("market_date")
            .rowsBetween(-29, 0)
        )

        rolling_fx_variance = F.var_samp("fx_close_change_pct").over(
            rolling_window
        )
        rolling_futures_variance = F.var_samp("futures_vnd_return_pct").over(
            rolling_window
        )

        aligned = (
            aligned
            .withColumn(
                "rolling30_pair_observation_count",
                F.sum(
                    F.when(
                        F.col("domestic_return_pct").isNotNull()
                        & F.col("fx_close_change_pct").isNotNull(),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ).over(rolling_window),
            )
            .withColumn(
                "rolling30_corr_domestic_fx",
                F.corr(
                    "domestic_return_pct", "fx_close_change_pct"
                ).over(rolling_window),
            )
            .withColumn(
                "rolling30_corr_domestic_futures_vnd",
                F.corr(
                    "domestic_return_pct", "futures_vnd_return_pct"
                ).over(rolling_window),
            )
            .withColumn(
                "rolling30_beta_domestic_on_fx",
                F.when(
                    rolling_fx_variance.isNotNull()
                    & (rolling_fx_variance > 0),
                    F.covar_samp(
                        "domestic_return_pct", "fx_close_change_pct"
                    ).over(rolling_window)
                    / rolling_fx_variance,
                ),
            )
            .withColumn(
                "rolling30_beta_domestic_on_futures_vnd",
                F.when(
                    rolling_futures_variance.isNotNull()
                    & (rolling_futures_variance > 0),
                    F.covar_samp(
                        "domestic_return_pct", "futures_vnd_return_pct"
                    ).over(rolling_window)
                    / rolling_futures_variance,
                ),
            )
        )

        # ------------------------------------------------------------------
        # 7. Explicit quality flags and final output schema.
        # ------------------------------------------------------------------
        aligned = aligned.withColumn(
            "quality_status",
            F.when(
                F.col("domestic_price_vnd_kg").isNull()
                | (F.col("domestic_price_vnd_kg") <= 0),
                F.lit("REVIEW_DOMESTIC_PRICE"),
            )
            .when(
                F.col("futures_price_usd_per_tonne").isNull()
                | (F.col("futures_price_usd_per_tonne") <= 0)
                | (F.col("futures_valid_price_count") <= 0),
                F.lit("REVIEW_FUTURES_PRICE"),
            )
            .when(
                F.col("fx_rate_vnd_per_usd").isNull(),
                F.lit("REVIEW_FX_MISSING"),
            )
            .when(
                F.col("fx_age_days") > MAX_FX_AGE_DAYS,
                F.lit("REVIEW_FX_STALE"),
            )
            .otherwise(F.lit("PASS")),
        )

        mart = aligned.select(
            "market_date",
            "province",
            "price_series_key",
            "source_family",
            "domestic_price_vnd_kg",
            "domestic_quality_status",
            "domestic_change_pct",
            "futures_price_usd_per_tonne",
            "futures_avg_change_pct",
            "futures_observation_count",
            "futures_valid_price_count",
            "futures_ohlc_review_count",
            "fx_currency_pair",
            "fx_source",
            "fx_rate_type",
            "fx_observation_date",
            "fx_rate_vnd_per_usd",
            "fx_age_days",
            "fx_close_change_pct",
            "fx_quality_status",
            "futures_price_vnd_per_kg",
            "basis_domestic_minus_futures_vnd_kg",
            "basis_pct_of_futures",
            "fx_effect_on_futures_vnd_kg",
            "domestic_return_pct",
            "futures_vnd_return_pct",
            "domestic_futures_return_gap_pp",
            "rolling30_pair_observation_count",
            "rolling30_corr_domestic_fx",
            "rolling30_corr_domestic_futures_vnd",
            "rolling30_beta_domestic_on_fx",
            "rolling30_beta_domestic_on_futures_vnd",
            "quality_status",
        )

        # ------------------------------------------------------------------
        # 8. Grain, completeness, FX alignment and value validation.
        # ------------------------------------------------------------------
        target_rows = mart.count()
        distinct_grain_keys = mart.select(
            "market_date", "province", "price_series_key"
        ).distinct().count()
        duplicate_grain_keys = target_rows - distinct_grain_keys
        null_key_rows = mart.filter(
            F.col("market_date").isNull()
            | F.col("province").isNull()
            | F.col("price_series_key").isNull()
        ).count()
        fx_missing_rows = mart.filter(
            F.col("fx_rate_vnd_per_usd").isNull()
        ).count()
        fx_carried_rows = mart.filter(
            F.col("fx_age_days") > 0
        ).count()
        fx_future_lookahead_rows = mart.filter(
            F.col("fx_observation_date") > F.col("market_date")
        ).count()
        invalid_converted_price_rows = mart.filter(
            F.col("futures_price_vnd_per_kg").isNotNull()
            & (F.col("futures_price_vnd_per_kg") <= 0)
        ).count()
        quality_review_rows = mart.filter(
            F.col("quality_status") != "PASS"
        ).count()
        max_fx_age = mart.agg(F.max("fx_age_days").alias("max_age")).first()["max_age"]

        date_range = mart.agg(
            F.min("market_date").alias("first_date"),
            F.max("market_date").alias("last_date"),
        ).first()

        print("DM15_TARGET_ROWS:", target_rows)
        print("DM15_DISTINCT_GRAIN_KEYS:", distinct_grain_keys)
        print("DM15_DUPLICATE_GRAIN_KEYS:", duplicate_grain_keys)
        print("DM15_NULL_GRAIN_ROWS:", null_key_rows)
        print("DM15_FX_MISSING_ROWS:", fx_missing_rows)
        print("DM15_FX_CARRIED_FORWARD_ROWS:", fx_carried_rows)
        print("DM15_FX_MAX_AGE_DAYS:", max_fx_age)
        print("DM15_FX_FUTURE_LOOKAHEAD_ROWS:", fx_future_lookahead_rows)
        print("DM15_INVALID_CONVERTED_FUTURES_ROWS:", invalid_converted_price_rows)
        print("DM15_QUALITY_REVIEW_ROWS:", quality_review_rows)
        print(
            "DM15_DATE_RANGE:",
            date_range["first_date"],
            "->",
            date_range["last_date"],
        )

        if target_rows != base_rows:
            raise RuntimeError(
                "As-of FX alignment changed the base row count. base_rows="
                + str(base_rows)
                + ", target_rows="
                + str(target_rows)
            )
        if target_rows != distinct_grain_keys or duplicate_grain_keys != 0:
            raise RuntimeError("DM15 target grain validation failed: duplicate keys.")
        if null_key_rows != 0:
            raise RuntimeError("DM15 target grain validation failed: null keys.")
        if fx_future_lookahead_rows != 0:
            raise RuntimeError("FX look-ahead detected; future rates must never be used.")
        if invalid_converted_price_rows != 0:
            raise RuntimeError("Invalid converted Futures prices detected.")

        print("\n========== DM15 SAMPLE: LATEST FX-ADJUSTED ROBUSTA PRICES ==========")
        mart.orderBy(F.col("market_date").desc()).select(
            "market_date",
            "province",
            "domestic_price_vnd_kg",
            "futures_price_usd_per_tonne",
            "fx_rate_vnd_per_usd",
            "fx_observation_date",
            "fx_age_days",
            "futures_price_vnd_per_kg",
            "basis_domestic_minus_futures_vnd_kg",
            "basis_pct_of_futures",
            "fx_effect_on_futures_vnd_kg",
            "rolling30_corr_domestic_fx",
            "rolling30_corr_domestic_futures_vnd",
            "quality_status",
        ).show(30, truncate=False)

        print("\nDM15_ROBUSTA_FX_PRICE_TRANSMISSION_VALIDATION_PASSED")

        if args.write:
            if spark.catalog.tableExists(TARGET_TABLE):
                raise RuntimeError(
                    "Target table already exists; refusing to overwrite: "
                    + TARGET_TABLE
                )

            (
                mart.writeTo(TARGET_TABLE)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .create()
            )

            iceberg_rows = spark.table(TARGET_TABLE).count()
            print("DM15_ICEBERG_ROWS:", iceberg_rows)
            if iceberg_rows != target_rows:
                raise RuntimeError(
                    "Iceberg row count mismatch. expected="
                    + str(target_rows)
                    + ", actual="
                    + str(iceberg_rows)
                )
            print("DM15_ICEBERG_WRITE_PASSED")

        print("SPARK_JOB_PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
