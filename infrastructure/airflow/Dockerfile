FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-standard==1.0.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
