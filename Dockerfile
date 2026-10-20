ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.14
FROM ${AIRFLOW_IMAGE}

ARG KUBECTL_VERSION=v1.35.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-cncf-kubernetes" \
    "apache-airflow-providers-postgres" \
    "psycopg[binary]"
