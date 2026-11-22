#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DM16 - Robusta ML Features Daily
Target: lakehouse.gold.mart_robusta_ml_features_daily

Grain:
    market_date + province + price_series_key

Forecasting convention:
    Features are available at the close of market_date (T).
    Targets are the next 1 and next 7 AVAILABLE OBSERVATIONS in the same
    province/price_series_key series, not necessarily calendar days.

Default mode is DRY RUN. Pass --write to create the Iceberg table.
The script intentionally refuses to overwrite an existing target table.
"""

import argparse
import math
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BASE_TABLE = "lakehouse.gold.mart_robusta_fx_price_transmission"
PRICE_TABLE = "lakehouse.gold.mart_coffee_price_trends"
WEATHER_TABLE = "lakehouse.gold.mart_weather_price_impact"
FUEL_TABLE = "lakehouse.gold.mart_fuel_price_impact"
TARGET_TABLE = "lakehouse.gold.mart_robusta_ml_features_daily"

# One canonical fuel series only:
# FUEL/DO_0_05S_II_V2 from BBW Fuel.
FUEL_SERIES_KEY = 194740289952944064
FUEL_SOURCE_FAMILY = "bbw_fuel"

GRAIN = ["market_date", "province", "price_series_key"]


def duplicate_key_count(df, keys):
    return (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def require_columns(df, table_name, columns):
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise RuntimeError(
            f"Missing columns in {table_name}: {', '.join(missing)}"
        )


def print_metric(name, value):
    print(f"{name}: {value}")


def main():
    parser = argparse.ArgumentParser(
        description="Build DM16 Robusta ML feature mart."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Create the Iceberg target table. Without this flag, run DRY RUN.",
    )
    args = parser.parse_args()

    mode = "WRITE" if args.write else "DRY RUN"
    print("=" * 78)
    print("DM16 - ROBUSTA ML FEATURES DAILY")
    print(f"BASE: {BASE_TABLE}")
    print(f"PRICE: {PRICE_TABLE}")
    print(f"WEATHER: {WEATHER_TABLE}")
    print(f"FUEL: {FUEL_TABLE}")
    print(f"FUEL_SERIES_KEY: {FUEL_SERIES_KEY}")
    print(f"FUEL_SOURCE_FAMILY: {FUEL_SOURCE_FAMILY}")
    print(f"TARGET: {TARGET_TABLE}")
    print("GRAIN: market_date + province + price_series_key")
    print("TARGET HORIZONS: next 1 and next 7 available observations")
    print(f"MODE: {mode}")
    print("=" * 78)


    spark = (
        SparkSession.builder
        .appName("DM16-Robusta-ML-Features-Daily")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.lakehouse.type",
            "rest"
        )
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    
    try:
        # ------------------------------------------------------------------
        # 1. Read and validate source tables
        # ------------------------------------------------------------------
        base_raw = spark.table(BASE_TABLE)
        price_raw = spark.table(PRICE_TABLE)
        weather_raw = spark.table(WEATHER_TABLE)
        fuel_raw = spark.table(FUEL_TABLE)

        require_columns(
            base_raw,
            BASE_TABLE,
            [
                "market_date", "province", "price_series_key", "source_family",
                "domestic_price_vnd_kg", "domestic_quality_status",
                "domestic_change_pct", "futures_price_usd_per_tonne",
                "futures_avg_change_pct", "futures_observation_count",
                "futures_valid_price_count", "futures_ohlc_review_count",
                "fx_currency_pair", "fx_source", "fx_rate_type",
                "fx_observation_date", "fx_rate_vnd_per_usd", "fx_age_days",
                "fx_close_change_pct", "fx_quality_status",
                "futures_price_vnd_per_kg",
                "basis_domestic_minus_futures_vnd_kg", "basis_pct_of_futures",
                "fx_effect_on_futures_vnd_kg", "domestic_return_pct",
                "futures_vnd_return_pct", "domestic_futures_return_gap_pp",
                "rolling30_pair_observation_count",
                "rolling30_corr_domestic_fx",
                "rolling30_corr_domestic_futures_vnd",
                "rolling30_beta_domestic_on_fx",
                "rolling30_beta_domestic_on_futures_vnd",
                "quality_status",
            ],
        )
        require_columns(
            price_raw,
            PRICE_TABLE,
            GRAIN + [
                "ma7_vnd_kg", "ma30_vnd_kg", "min30_vnd_kg",
                "max30_vnd_kg", "volatility30_vnd_kg",
            ],
        )
        require_columns(
            weather_raw,
            WEATHER_TABLE,
            GRAIN + [
                "temperature_mean_c", "rain_sum_mm", "precipitation_mm",
                "weather_station_count", "weather_is_corrected",
                "weather_review_count", "rain_price_corr30",
                "temperature_price_corr30",
            ],
        )
        require_columns(
            fuel_raw,
            FUEL_TABLE,
            GRAIN + [
                "fuel_series_key", "fuel_series_code", "fuel_source_family",
                "fuel_effective_date", "fuel_price_vnd_per_unit",
                "fuel_change_pct", "fuel_quality_status", "fuel_age_days",
                "is_fuel_update_day", "price_jump_review",
            ],
        )

        base = base_raw.select(
            "market_date",
            "province",
            "price_series_key",
            "source_family",
            F.col("domestic_price_vnd_kg").cast("double").alias(
                "domestic_price_vnd_kg"
            ),
            "domestic_quality_status",
            F.col("domestic_change_pct").cast("double").alias(
                "domestic_change_pct"
            ),
            F.col("futures_price_usd_per_tonne").cast("double").alias(
                "futures_price_usd_per_tonne"
            ),
            F.col("futures_avg_change_pct").cast("double").alias(
                "futures_avg_change_pct"
            ),
            "futures_observation_count",
            "futures_valid_price_count",
            "futures_ohlc_review_count",
            "fx_currency_pair",
            "fx_source",
            "fx_rate_type",
            "fx_observation_date",
            F.col("fx_rate_vnd_per_usd").cast("double").alias(
                "fx_rate_vnd_per_usd"
            ),
            "fx_age_days",
            F.col("fx_close_change_pct").cast("double").alias(
                "fx_close_change_pct"
            ),
            "fx_quality_status",
            F.col("futures_price_vnd_per_kg").cast("double").alias(
                "futures_price_vnd_per_kg"
            ),
            F.col("basis_domestic_minus_futures_vnd_kg").cast("double").alias(
                "basis_domestic_minus_futures_vnd_kg"
            ),
            F.col("basis_pct_of_futures").cast("double").alias(
                "basis_pct_of_futures"
            ),
            F.col("fx_effect_on_futures_vnd_kg").cast("double").alias(
                "fx_effect_on_futures_vnd_kg"
            ),
            F.col("domestic_return_pct").cast("double").alias(
                "domestic_return_pct"
            ),
            F.col("futures_vnd_return_pct").cast("double").alias(
                "futures_vnd_return_pct"
            ),
            F.col("domestic_futures_return_gap_pp").cast("double").alias(
                "domestic_futures_return_gap_pp"
            ),
            "rolling30_pair_observation_count",
            F.col("rolling30_corr_domestic_fx").cast("double").alias(
                "rolling30_corr_domestic_fx"
            ),
            F.col("rolling30_corr_domestic_futures_vnd").cast("double").alias(
                "rolling30_corr_domestic_futures_vnd"
            ),
            F.col("rolling30_beta_domestic_on_fx").cast("double").alias(
                "rolling30_beta_domestic_on_fx"
            ),
            F.col("rolling30_beta_domestic_on_futures_vnd").cast("double").alias(
                "rolling30_beta_domestic_on_futures_vnd"
            ),
            "quality_status",
        )

        base_rows = base.count()
        base_duplicates = duplicate_key_count(base, GRAIN)
        base_null_keys = base.filter(
            F.col("market_date").isNull()
            | F.col("province").isNull()
            | F.col("price_series_key").isNull()
        ).count()

        print_metric("DM16_BASE_ROWS", base_rows)
        print_metric("DM16_BASE_DUPLICATE_GRAIN_KEYS", base_duplicates)
        print_metric("DM16_BASE_NULL_GRAIN_ROWS", base_null_keys)

        if base_rows == 0:
            raise RuntimeError("DM15 base table is empty.")
        if base_duplicates != 0 or base_null_keys != 0:
            raise RuntimeError("DM15 base grain validation failed.")

        # ------------------------------------------------------------------
        # 2. Prepare one-row-per-grain feature inputs
        # ------------------------------------------------------------------
        price = price_raw.select(
            *GRAIN,
            F.col("ma7_vnd_kg").cast("double").alias("price_ma7_vnd_kg"),
            F.col("ma30_vnd_kg").cast("double").alias("price_ma30_vnd_kg"),
            F.col("min30_vnd_kg").cast("double").alias("price_min30_vnd_kg"),
            F.col("max30_vnd_kg").cast("double").alias("price_max30_vnd_kg"),
            F.col("volatility30_vnd_kg").cast("double").alias(
                "price_volatility30_vnd_kg"
            ),
        )

        weather = weather_raw.select(
            *GRAIN,
            F.col("temperature_mean_c").cast("double").alias(
                "weather_temperature_mean_c"
            ),
            F.col("rain_sum_mm").cast("double").alias("weather_rain_sum_mm"),
            F.col("precipitation_mm").cast("double").alias(
                "weather_precipitation_mm"
            ),
            F.col("weather_station_count").cast("long").alias(
                "weather_station_count"
            ),
            "weather_is_corrected",
            F.col("weather_review_count").cast("long").alias(
                "weather_review_count"
            ),
            F.col("rain_price_corr30").cast("double").alias(
                "weather_rain_price_corr30"
            ),
            F.col("temperature_price_corr30").cast("double").alias(
                "weather_temperature_price_corr30"
            ),
        )

        # Filter to exactly one canonical BBW diesel series.
        # Effective date must not be later than the market date.
        fuel = (
            fuel_raw
            .filter(
                (F.col("fuel_series_key") == F.lit(FUEL_SERIES_KEY))
                & (F.col("fuel_source_family") == F.lit(FUEL_SOURCE_FAMILY))
                & (F.col("fuel_effective_date") <= F.col("market_date"))
            )
            .select(
                *GRAIN,
                "fuel_series_key",
                "fuel_series_code",
                "fuel_region",
                "fuel_unit",
                "fuel_effective_date",
                F.col("fuel_price_vnd_per_unit").cast("double").alias(
                    "fuel_price_vnd_per_unit"
                ),
                F.col("fuel_change_pct").cast("double").alias(
                    "fuel_change_pct"
                ),
                "fuel_quality_status",
                "fuel_age_days",
                "is_fuel_update_day",
                "price_jump_review",
            )
        )

        price_duplicates = duplicate_key_count(price, GRAIN)
        weather_duplicates = duplicate_key_count(weather, GRAIN)
        fuel_duplicates = duplicate_key_count(fuel, GRAIN)
        fuel_rows = fuel.count()

        print_metric("DM16_PRICE_SOURCE_ROWS", price.count())
        print_metric("DM16_PRICE_DUPLICATE_GRAIN_KEYS", price_duplicates)
        print_metric("DM16_WEATHER_SOURCE_ROWS", weather.count())
        print_metric("DM16_WEATHER_DUPLICATE_GRAIN_KEYS", weather_duplicates)
        print_metric("DM16_SELECTED_FUEL_ROWS", fuel_rows)
        print_metric("DM16_SELECTED_FUEL_DUPLICATE_GRAIN_KEYS", fuel_duplicates)

        if price_duplicates != 0:
            raise RuntimeError("Price-trend mart has duplicate grain keys.")
        if weather_duplicates != 0:
            raise RuntimeError("Weather mart has duplicate grain keys.")
        if fuel_rows == 0:
            raise RuntimeError(
                "Selected BBW diesel series was not found in mart_fuel_price_impact."
            )
        if fuel_duplicates != 0:
            raise RuntimeError(
                "Selected fuel series has duplicate grain keys; refusing fan-out JOIN."
            )

        # ------------------------------------------------------------------
        # 3. Left joins: keep the exact 8,124-row DM15 population
        # ------------------------------------------------------------------
        joined = (
            base
            .join(price, on=GRAIN, how="left")
            .join(weather, on=GRAIN, how="left")
            .join(fuel, on=GRAIN, how="left")
        )

        joined_rows = joined.count()
        if joined_rows != base_rows:
            raise RuntimeError(
                "JOIN changed row count: "
                f"base={base_rows}, joined={joined_rows}. Possible fan-out."
            )

        print_metric("DM16_ROWS_AFTER_LEFT_JOINS", joined_rows)
        print_metric(
            "DM16_WEATHER_MATCHED_ROWS",
            joined.filter(F.col("weather_temperature_mean_c").isNotNull()).count(),
        )
        print_metric(
            "DM16_FUEL_MATCHED_ROWS",
            joined.filter(F.col("fuel_price_vnd_per_unit").isNotNull()).count(),
        )

        # ------------------------------------------------------------------
        # 4. Time-series features and supervised targets
        # ------------------------------------------------------------------
        w_order = (
            Window
            .partitionBy("province", "price_series_key")
            .orderBy("market_date")
        )
        w7 = w_order.rowsBetween(-6, 0)
        w30 = w_order.rowsBetween(-29, 0)

        features = (
            joined
            .withColumn(
                "previous_observation_date",
                F.lag("market_date", 1).over(w_order),
            )
            .withColumn(
                "days_since_prev_observation",
                F.datediff(
                    F.col("market_date"),
                    F.col("previous_observation_date"),
                ),
            )
            .withColumn(
                "price_lag_1_obs_vnd_kg",
                F.lag("domestic_price_vnd_kg", 1).over(w_order),
            )
            .withColumn(
                "price_lag_7_obs_vnd_kg",
                F.lag("domestic_price_vnd_kg", 7).over(w_order),
            )
            .withColumn(
                "price_lag_14_obs_vnd_kg",
                F.lag("domestic_price_vnd_kg", 14).over(w_order),
            )
            .withColumn(
                "price_lag_30_obs_vnd_kg",
                F.lag("domestic_price_vnd_kg", 30).over(w_order),
            )
            .withColumn(
                "price_rolling_mean_7_obs_vnd_kg",
                F.avg("domestic_price_vnd_kg").over(w7),
            )
            .withColumn(
                "price_rolling_mean_30_obs_vnd_kg",
                F.avg("domestic_price_vnd_kg").over(w30),
            )
            .withColumn(
                "price_rolling_std_7_obs_vnd_kg",
                F.stddev_samp("domestic_price_vnd_kg").over(w7),
            )
            .withColumn(
                "price_rolling_std_30_obs_vnd_kg",
                F.stddev_samp("domestic_price_vnd_kg").over(w30),
            )
            .withColumn(
                "price_rolling_min_30_obs_vnd_kg",
                F.min("domestic_price_vnd_kg").over(w30),
            )
            .withColumn(
                "price_rolling_max_30_obs_vnd_kg",
                F.max("domestic_price_vnd_kg").over(w30),
            )
            # Weather is shifted by one available observation. This avoids
            # using same-day realized weather as a predictor of the next price.
            .withColumn(
                "weather_temperature_mean_lag_1_obs_c",
                F.lag("weather_temperature_mean_c", 1).over(w_order),
            )
            .withColumn(
                "weather_rain_sum_lag_1_obs_mm",
                F.lag("weather_rain_sum_mm", 1).over(w_order),
            )
            .withColumn(
                "weather_precipitation_lag_1_obs_mm",
                F.lag("weather_precipitation_mm", 1).over(w_order),
            )
            .withColumn(
                "weather_station_count_lag_1_obs",
                F.lag("weather_station_count", 1).over(w_order),
            )
            .withColumn(
                "weather_review_count_lag_1_obs",
                F.lag("weather_review_count", 1).over(w_order),
            )
            .withColumn(
                "weather_rain_price_corr30_lag_1_obs",
                F.lag("weather_rain_price_corr30", 1).over(w_order),
            )
            .withColumn(
                "weather_temperature_price_corr30_lag_1_obs",
                F.lag("weather_temperature_price_corr30", 1).over(w_order),
            )
            # Labels: next 1 and next 7 available observations.
            .withColumn(
                "target_next_1_obs_date",
                F.lead("market_date", 1).over(w_order),
            )
            .withColumn(
                "target_next_1_obs_price_vnd_kg",
                F.lead("domestic_price_vnd_kg", 1).over(w_order),
            )
            .withColumn(
                "target_next_7_obs_date",
                F.lead("market_date", 7).over(w_order),
            )
            .withColumn(
                "target_next_7_obs_price_vnd_kg",
                F.lead("domestic_price_vnd_kg", 7).over(w_order),
            )
        )

        features = (
            features
            .withColumn(
                "target_next_1_obs_return_pct",
                F.when(
                    F.col("domestic_price_vnd_kg") > 0,
                    (
                        F.col("target_next_1_obs_price_vnd_kg")
                        / F.col("domestic_price_vnd_kg") - F.lit(1.0)
                    ) * F.lit(100.0),
                ),
            )
            .withColumn(
                "target_next_7_obs_return_pct",
                F.when(
                    F.col("domestic_price_vnd_kg") > 0,
                    (
                        F.col("target_next_7_obs_price_vnd_kg")
                        / F.col("domestic_price_vnd_kg") - F.lit(1.0)
                    ) * F.lit(100.0),
                ),
            )
            .withColumn("calendar_year", F.year("market_date").cast("int"))
            .withColumn("calendar_month", F.month("market_date").cast("int"))
            .withColumn("calendar_quarter", F.quarter("market_date").cast("int"))
            .withColumn("calendar_day_of_week", F.dayofweek("market_date").cast("int"))
            .withColumn("calendar_day_of_year", F.dayofyear("market_date").cast("int"))
            .withColumn(
                "calendar_month_sin",
                F.sin(
                    F.lit(2.0 * math.pi)
                    * F.col("calendar_month") / F.lit(12.0)
                ),
            )
            .withColumn(
                "calendar_month_cos",
                F.cos(
                    F.lit(2.0 * math.pi)
                    * F.col("calendar_month") / F.lit(12.0)
                ),
            )
            .withColumn(
                "calendar_day_of_year_sin",
                F.sin(
                    F.lit(2.0 * math.pi)
                    * F.col("calendar_day_of_year") / F.lit(365.25)
                ),
            )
            .withColumn(
                "calendar_day_of_year_cos",
                F.cos(
                    F.lit(2.0 * math.pi)
                    * F.col("calendar_day_of_year") / F.lit(365.25)
                ),
            )
            .withColumn(
                "is_model_eligible_t1",
                (
                    (F.col("quality_status") == F.lit("PASS"))
                    & (F.col("domestic_quality_status") == F.lit("PASS"))
                    & (F.col("domestic_price_vnd_kg") > 0)
                    & (F.col("target_next_1_obs_price_vnd_kg") > 0)
                ),
            )
            .withColumn(
                "is_model_eligible_t7",
                (
                    (F.col("quality_status") == F.lit("PASS"))
                    & (F.col("domestic_quality_status") == F.lit("PASS"))
                    & (F.col("domestic_price_vnd_kg") > 0)
                    & (F.col("target_next_7_obs_price_vnd_kg") > 0)
                ),
            )
            .withColumn("feature_version", F.lit("DM16_V1"))
            .withColumn("gold_processed_at", F.current_timestamp())
            .withColumn(
                "feature_fact_key",
                F.xxhash64(
                    F.col("market_date").cast("string"),
                    F.col("province"),
                    F.col("price_series_key").cast("string"),
                ),
            )
        )

        output_columns = [
            "feature_fact_key",
            "market_date",
            "province",
            "price_series_key",
            "source_family",
            "domestic_price_vnd_kg",
            "domestic_quality_status",
            "quality_status",
            "domestic_change_pct",
            "domestic_return_pct",
            "price_lag_1_obs_vnd_kg",
            "price_lag_7_obs_vnd_kg",
            "price_lag_14_obs_vnd_kg",
            "price_lag_30_obs_vnd_kg",
            "price_ma7_vnd_kg",
            "price_ma30_vnd_kg",
            "price_volatility30_vnd_kg",
            "price_min30_vnd_kg",
            "price_max30_vnd_kg",
            "price_rolling_mean_7_obs_vnd_kg",
            "price_rolling_mean_30_obs_vnd_kg",
            "price_rolling_std_7_obs_vnd_kg",
            "price_rolling_std_30_obs_vnd_kg",
            "price_rolling_min_30_obs_vnd_kg",
            "price_rolling_max_30_obs_vnd_kg",
            "days_since_prev_observation",
            "futures_price_usd_per_tonne",
            "futures_price_vnd_per_kg",
            "futures_avg_change_pct",
            "futures_vnd_return_pct",
            "futures_observation_count",
            "futures_valid_price_count",
            "futures_ohlc_review_count",
            "basis_domestic_minus_futures_vnd_kg",
            "basis_pct_of_futures",
            "fx_effect_on_futures_vnd_kg",
            "domestic_futures_return_gap_pp",
            "rolling30_pair_observation_count",
            "rolling30_corr_domestic_fx",
            "rolling30_corr_domestic_futures_vnd",
            "rolling30_beta_domestic_on_fx",
            "rolling30_beta_domestic_on_futures_vnd",
            "fx_currency_pair",
            "fx_source",
            "fx_rate_type",
            "fx_observation_date",
            "fx_rate_vnd_per_usd",
            "fx_age_days",
            "fx_close_change_pct",
            "fx_quality_status",
            "weather_temperature_mean_lag_1_obs_c",
            "weather_rain_sum_lag_1_obs_mm",
            "weather_precipitation_lag_1_obs_mm",
            "weather_station_count_lag_1_obs",
            "weather_review_count_lag_1_obs",
            "weather_rain_price_corr30_lag_1_obs",
            "weather_temperature_price_corr30_lag_1_obs",
            "fuel_series_key",
            "fuel_series_code",
            "fuel_region",
            "fuel_unit",
            "fuel_effective_date",
            "fuel_price_vnd_per_unit",
            "fuel_change_pct",
            "fuel_quality_status",
            "fuel_age_days",
            "is_fuel_update_day",
            "price_jump_review",
            "calendar_year",
            "calendar_month",
            "calendar_quarter",
            "calendar_day_of_week",
            "calendar_day_of_year",
            "calendar_month_sin",
            "calendar_month_cos",
            "calendar_day_of_year_sin",
            "calendar_day_of_year_cos",
            "target_next_1_obs_date",
            "target_next_1_obs_price_vnd_kg",
            "target_next_1_obs_return_pct",
            "target_next_7_obs_date",
            "target_next_7_obs_price_vnd_kg",
            "target_next_7_obs_return_pct",
            "is_model_eligible_t1",
            "is_model_eligible_t7",
            "feature_version",
            "gold_processed_at",
        ]

        result = features.select(*output_columns)

        # ------------------------------------------------------------------
        # 5. Data-quality and leakage checks
        # ------------------------------------------------------------------
        result_rows = result.count()
        result_duplicates = duplicate_key_count(result, GRAIN)
        result_null_keys = result.filter(
            F.col("market_date").isNull()
            | F.col("province").isNull()
            | F.col("price_series_key").isNull()
        ).count()

        target1_bad_dates = result.filter(
            F.col("target_next_1_obs_date").isNotNull()
            & (F.col("target_next_1_obs_date") <= F.col("market_date"))
        ).count()
        target7_bad_dates = result.filter(
            F.col("target_next_7_obs_date").isNotNull()
            & (F.col("target_next_7_obs_date") <= F.col("market_date"))
        ).count()
        fuel_future_rows = result.filter(
            F.col("fuel_effective_date").isNotNull()
            & (F.col("fuel_effective_date") > F.col("market_date"))
        ).count()

        target1_nulls = result.filter(
            F.col("target_next_1_obs_price_vnd_kg").isNull()
        ).count()
        target7_nulls = result.filter(
            F.col("target_next_7_obs_price_vnd_kg").isNull()
        ).count()
        eligible_t1 = result.filter(F.col("is_model_eligible_t1")).count()
        eligible_t7 = result.filter(F.col("is_model_eligible_t7")).count()

        print_metric("DM16_TARGET_ROWS", result_rows)
        print_metric("DM16_DISTINCT_GRAIN_KEYS", result.select(*GRAIN).distinct().count())
        print_metric("DM16_DUPLICATE_GRAIN_KEYS", result_duplicates)
        print_metric("DM16_NULL_GRAIN_ROWS", result_null_keys)
        print_metric("DM16_TARGET_T1_NULL_ROWS", target1_nulls)
        print_metric("DM16_TARGET_T7_NULL_ROWS", target7_nulls)
        print_metric("DM16_MODEL_ELIGIBLE_T1_ROWS", eligible_t1)
        print_metric("DM16_MODEL_ELIGIBLE_T7_ROWS", eligible_t7)
        print_metric("DM16_TARGET_T1_NONFUTURE_DATE_ROWS", target1_bad_dates)
        print_metric("DM16_TARGET_T7_NONFUTURE_DATE_ROWS", target7_bad_dates)
        print_metric("DM16_FUEL_FUTURE_LOOKAHEAD_ROWS", fuel_future_rows)

        if result_rows != base_rows:
            raise RuntimeError(
                f"Target row count mismatch: base={base_rows}, target={result_rows}"
            )
        if result_duplicates != 0 or result_null_keys != 0:
            raise RuntimeError("DM16 grain validation failed.")
        if target1_bad_dates != 0 or target7_bad_dates != 0:
            raise RuntimeError("Target date leakage validation failed.")
        if fuel_future_rows != 0:
            raise RuntimeError("Fuel future-lookahead validation failed.")

        date_range = result.agg(
            F.min("market_date").alias("min_date"),
            F.max("market_date").alias("max_date"),
        ).first()
        print_metric(
            "DM16_DATE_RANGE",
            f"{date_range['min_date']} -> {date_range['max_date']}",
        )

        print("\n========== DM16 SAMPLE: LATEST FEATURE ROWS ==========")
        result.orderBy(F.col("market_date").desc()).show(10, truncate=False)

        if not args.write:
            print("DM16_DRY_RUN_PASSED")
            print("No Iceberg table was written. Use --write after reviewing the output.")
            return

        # ------------------------------------------------------------------
        # 6. Write Iceberg table only when explicitly requested
        # ------------------------------------------------------------------
        if spark.catalog.tableExists(TARGET_TABLE):
            raise RuntimeError(
                f"Target table already exists: {TARGET_TABLE}. "
                "Refusing to overwrite it."
            )

        result.writeTo(TARGET_TABLE).using("iceberg").create()

        written_rows = spark.table(TARGET_TABLE).count()
        print_metric("DM16_ICEBERG_ROWS", written_rows)

        if written_rows != result_rows:
            raise RuntimeError(
                f"Iceberg row-count mismatch: expected={result_rows}, "
                f"actual={written_rows}"
            )

        print("DM16_ICEBERG_WRITE_PASSED")
        print("SPARK_JOB_PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
