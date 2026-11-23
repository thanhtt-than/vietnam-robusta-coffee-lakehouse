"""First Airflow-to-Spark connectivity DAG for the TLCN lakehouse.

This DAG is intentionally a non-mutating smoke test:
- Airflow (running in Ubuntu/WSL2) invokes the Docker CLI.
- Docker CLI executes spark-submit inside the existing tlcn-spark container.
- The Spark script checks Spark + the Iceberg/Nessie catalog without writing tables.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


SPARK_CONTAINER = "tlcn-spark"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
ICEBERG_JARS = (
    "/workspace/infrastructure/iceberg/jars/"
    "iceberg-spark-runtime-3.5_2.12-1.10.1.jar,"
    "/workspace/infrastructure/iceberg/jars/iceberg-aws-bundle-1.10.1.jar"
)
HEALTHCHECK_SCRIPT = (
    "/workspace/infrastructure/airflow/spark_airflow_healthcheck.py"
)


with DAG(
    dag_id="tlcn_airflow_spark_iceberg_connection_test",
    description="Verify Airflow -> Docker -> Spark -> Iceberg/Nessie connectivity",
    start_date=datetime(2026, 11, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["tlcn", "spark", "iceberg", "airflow3"],
) as dag:

    check_docker_and_spark_container = BashOperator(
        task_id="check_docker_and_spark_container",
        bash_command=f"""
set -euo pipefail

command -v docker >/dev/null 2>&1 || {{
    echo "DOCKER_CLI_NOT_FOUND_IN_AIRFLOW_ENV"
    exit 127
}}

docker exec {SPARK_CONTAINER} test -x {SPARK_SUBMIT}
docker exec {SPARK_CONTAINER} test -f {HEALTHCHECK_SCRIPT}

echo "AIRFLOW_DOCKER_SPARK_CONTAINER_CHECK_PASSED"
""",
        execution_timeout=timedelta(minutes=2),
    )

    run_spark_iceberg_healthcheck = BashOperator(
        task_id="run_spark_iceberg_healthcheck",
        bash_command=f"""
set -euo pipefail

docker exec {SPARK_CONTAINER} {SPARK_SUBMIT} \
    --master 'local[2]' \
    --jars '{ICEBERG_JARS}' \
    '{HEALTHCHECK_SCRIPT}'

echo "AIRFLOW_SPARK_ICEBERG_HEALTHCHECK_PASSED"
""",
        execution_timeout=timedelta(minutes=10),
    )

    check_docker_and_spark_container >> run_spark_iceberg_healthcheck
