ARG AIRFLOW_BASE_IMAGE="apache/latest"
FROM ${AIRFLOW_BASE_IMAGE}

USER root
RUN mkdir -p /data && chown airflow /data

USER airflow
