"""TLCN Lakehouse bootstrap DAG for Airflow 3.4.0.
Runs existing jobs through Docker CLI -> tlcn-spark -> spark-submit.
schedule=None until the first end-to-end bootstrap succeeds.
The ML feature mart is deliberately excluded.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import shlex

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
CONTAINER = "tlcn-spark"
SUBMIT = "/opt/spark/bin/spark-submit"
JARS = (
    "/workspace/infrastructure/iceberg/jars/"
    "iceberg-spark-runtime-3.5_2.12-1.10.1.jar,"
    "/workspace/infrastructure/iceberg/jars/iceberg-aws-bundle-1.10.1.jar"
)
GUARD_SCRIPT = "infrastructure/airflow-v3/spark_table_guard.py"
GUARD_PATH = "/workspace/" + GUARD_SCRIPT

def spark(script, args=()):
    return shlex.join([
        "docker", "exec", CONTAINER, SUBMIT, "--master", "local[2]",
        "--jars", JARS, "/workspace/" + script, *args
    ])

def python_job(script, args=()):
    """Run project Python scripts as modules so absolute src.* imports resolve."""
    module = script.removesuffix(".py").replace("/", ".")
    return shlex.join([
        "docker", "exec", "-w", "/workspace",
        "-e", "PYTHONPATH=/workspace",
        "-e", "PYTHONUNBUFFERED=1",
        CONTAINER,
        "python3", "-m", module, *args
    ])

def task(task_id, command, minutes=60):
    return BashOperator(
        task_id=task_id,
        bash_command="set -euo pipefail\n" + command + "\n",
        execution_timeout=timedelta(minutes=minutes),
    )

def create_mart_if_missing(task_id, script, target):
    guard_cmd = spark(GUARD_SCRIPT, [target])
    build_cmd = spark(script, ["--write"])
    cmd = f"""
guard_output="$({guard_cmd} 2>&1)" || {{
  printf '%s\\n' "$guard_output"
  echo "MART_GUARD_FAILED: {target}"
  exit 1
}}
printf '%s\\n' "$guard_output"
if printf '%s' "$guard_output" | grep -q 'TLCN_TABLE_EXISTS'; then
  echo "MART_ALREADY_EXISTS_SKIPPED: {target}"
elif printf '%s' "$guard_output" | grep -q 'TLCN_TABLE_MISSING'; then
  {build_cmd}
  echo "MART_CREATED: {target}"
else
  echo "MART_GUARD_UNKNOWN_RESULT: {target}"
  exit 1
fi
"""
    return task(task_id, cmd, 90)

with DAG(
    dag_id="tlcn_full_lakehouse_pipeline",
    description="Dynamic ingestion -> Bronze -> Silver -> Gold -> Data Marts",
    start_date=datetime(2026, 11, 20, tzinfo=TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={"owner": "tlcn", "depends_on_past": False, "retries": 0},
    tags=["tlcn", "lakehouse", "spark", "iceberg", "nessie", "airflow3"],
) as dag:
    preflight = task(
        "preflight_docker_spark",
        f"""command -v docker >/dev/null
docker inspect {CONTAINER} >/dev/null
docker exec {CONTAINER} test -x {SUBMIT}
docker exec {CONTAINER} test -f {GUARD_PATH}
docker exec {CONTAINER} test -f /workspace/data/open-meteo-12.48N107.97E419m_DakNong.csv
docker exec {CONTAINER} test -f /workspace/data/open-meteo-12.83N108.13E481m_DaKLak.csv
docker exec -w /workspace -e PYTHONPATH=/workspace {CONTAINER} python3 -c 'import requests, bs4, minio, dotenv; from src.bronze.minio_client import get_minio_client, get_bucket_name; assert get_minio_client().bucket_exists(get_bucket_name()), "MinIO bucket unavailable"'
echo TLCN_INGESTION_PREFLIGHT_PASSED
echo TLCN_PIPELINE_PREFLIGHT_PASSED""",
        2,
    )

    # Four refreshable sources. Historical/static Bronze sources are retained.
    with TaskGroup(group_id="dynamic_source_ingestion") as ingestion:
        ingest_tasks = [
            task("ingest_bbw_coffee", python_job(
                "src/bronze/ingest_bbw_coffee.py", ["--refresh-days", "3"]), 60),
            task("ingest_bbw_fuel", python_job(
                "src/bronze/ingest_bbw_fuel.py", ["--refresh-days", "3"]), 45),
            task("ingest_open_meteo", python_job(
                "src/bronze/ingest_open_meteo.py",
                ["--station", "all", "--refresh-days", "3"]), 90),
            task("crawl_vicofa", python_job(
                "src/bronze/crawl_vicofa.py", ["--allow-stale"]), 45),
        ]
        chain(*ingest_tasks)

    # Refresh the dynamic Bronze domains; the other historical Bronze tables
    # remain in scope downstream and are not reloaded from source files.
    with TaskGroup(group_id="dynamic_bronze_iceberg") as bronze:
        bronze_tasks = [
            task("bronze_coffee_prices", spark("src/iceberg/bronze_coffee_prices.py")),
            task("bronze_domestic_prices", spark("src/iceberg/bronze_domestic_prices.py")),
            task("bronze_fuel", spark("src/iceberg/bronze_fuel.py")),
            task("bronze_weather", spark("src/iceberg/bronze_weather.py")),
            task("bronze_vicofa", spark("src/iceberg/bronze_vicofa.py")),
        ]
        chain(*bronze_tasks)

    with TaskGroup(group_id="silver_transformations") as silver:
        silver_specs = [
            ("silver_coffee_new_sources", "src/iceberg/silver_coffee_new_sources.py"),
            ("silver_coffee_prices", "src/iceberg/silver_coffee_prices.py"),
            ("silver_coffee_trade_monthly", "src/iceberg/silver_coffee_trade_monthly.py"),
            ("silver_exchange_rate", "src/iceberg/silver_exchange_rate.py"),
            ("silver_fuel_prices", "src/iceberg/silver_fuel_prices.py"),
            ("silver_historical_stats", "src/iceberg/silver_historical_stats.py"),
            ("silver_london_futures", "src/iceberg/silver_london_futures.py"),
            ("silver_robusta_stock", "src/iceberg/silver_robusta_stock.py"),
            ("silver_vicofa_articles", "src/iceberg/silver_vicofa_articles.py"),
            ("silver_weather_daily", "src/iceberg/silver_weather_daily.py"),
        ]
        chain(*[
            task(tid, spark(path, ["--write"]))
            for tid, path in silver_specs
        ])

    with TaskGroup(group_id="gold_dimensions") as dimensions:
        specs = [
            ("gold_foundation_dimensions", "src/iceberg/gold_foundation_dimensions.py", ["--write"]),
            ("gold_market_dimensions", "src/iceberg/gold_market_dimensions.py", ["--write"]),
            ("gold_content_dimensions", "src/iceberg/gold_content_dimensions.py", ["--write"]),
            ("gold_production_dimensions", "src/iceberg/gold_production_dimensions.py", ["--write"]),
            ("gold_dim_currency_pair", "src/iceberg/gold_dim_currency_pair.py", []),
            ("gold_add_fx_sources", "src/iceberg/gold_add_fx_sources.py", []),
            ("gold_add_trade_dimensions", "src/iceberg/gold_add_trade_dimensions.py", ["--write"]),
        ]
        chain(*[task(tid, spark(path, args)) for tid, path, args in specs])

    with TaskGroup(group_id="gold_facts") as facts:
        fact_specs = [
            ("gold_fact_domestic_price", "src/iceberg/gold_fact_domestic_price.py"),
            ("gold_fact_exchange_rate", "src/iceberg/gold_fact_exchange_rate.py"),
            ("gold_fact_fuel_price", "src/iceberg/gold_fact_fuel_price.py"),
            ("gold_fact_futures_price", "src/iceberg/gold_fact_futures_price.py"),
            ("gold_fact_coffee_trade_monthly", "src/iceberg/gold_fact_coffee_trade_monthly.py"),
            ("gold_fact_export", "src/iceberg/gold_fact_export.py"),
            ("gold_fact_production", "src/iceberg/gold_fact_production_v2_staging.py"),
            ("gold_fact_psd", "src/iceberg/gold_fact_psd.py"),
            ("gold_fact_robusta_stock_port", "src/iceberg/gold_fact_robusta_stock_port.py"),
            ("gold_fact_robusta_stock_totals", "src/iceberg/gold_fact_robusta_stock_totals.py"),
            ("gold_fact_weather_daily", "src/iceberg/gold_fact_weather_daily.py"),
            ("gold_fact_news_topic", "src/iceberg/gold_fact_news_topic.py"),
        ]
        chain(*[
            task(tid, spark(path, ["--write"]))
            for tid, path in fact_specs
        ])

    with TaskGroup(group_id="gold_data_marts") as marts:
        mart_specs = [
            ("mart_coffee_market_daily", "src/gold/marts/build_mart_coffee_market_daily.py", "lakehouse.gold.mart_coffee_market_daily"),
            ("mart_coffee_price_trends", "src/gold/marts/build_mart_coffee_price_trends.py", "lakehouse.gold.mart_coffee_price_trends"),
            ("mart_coffee_production_annual", "src/gold/marts/build_mart_coffee_production_annual.py", "lakehouse.gold.mart_coffee_production_annual"),
            ("mart_coffee_supply_demand", "src/gold/marts/build_mart_coffee_supply_demand.py", "lakehouse.gold.mart_coffee_supply_demand"),
            ("mart_coffee_supply_demand_snapshot", "src/gold/marts/build_mart_coffee_supply_demand_snapshot.py", "lakehouse.gold.mart_coffee_supply_demand_snapshot"),
            ("mart_coffee_trade_monthly", "src/gold/marts/build_mart_coffee_trade_monthly.py", "lakehouse.gold.mart_coffee_trade_monthly"),
            ("mart_fuel_price_impact", "src/gold/marts/build_mart_fuel_price_impact.py", "lakehouse.gold.mart_fuel_price_impact"),
            ("mart_futures_domestic_spread", "src/gold/marts/build_mart_futures_domestic_spread.py", "lakehouse.gold.mart_futures_domestic_spread"),
            ("mart_market_news_analytics", "src/gold/marts/build_mart_market_news_analytics.py", "lakehouse.gold.mart_market_news_analytics"),
            ("mart_robusta_fx_price_transmission", "src/gold/marts/build_mart_robusta_fx_price_transmission.py", "lakehouse.gold.mart_robusta_fx_price_transmission"),
            ("mart_robusta_stock_analysis", "src/gold/marts/build_mart_robusta_stock_analysis.py", "lakehouse.gold.mart_robusta_stock_analysis"),
            ("mart_weather_price_impact", "src/gold/marts/build_mart_weather_price_impact.py", "lakehouse.gold.mart_weather_price_impact"),
            ("mart_coffee_export_annual", "src/gold/marts/build_mart_coffee_export_annual.py", "lakehouse.gold.mart_coffee_export_annual"),
        ]
        chain(*[
            create_mart_if_missing(tid, path, target)
            for tid, path, target in mart_specs
        ])

    preflight >> ingestion >> bronze >> silver >> dimensions >> facts >> marts
