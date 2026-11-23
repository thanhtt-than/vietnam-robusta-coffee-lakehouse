
FROM apache/airflow:3.4.0-python3.12

USER root

# Install Docker CLI only.
# Docker Engine is already provided by Docker Desktop.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker-cli \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.12

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-fab \
    apache-airflow-providers-standard \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
