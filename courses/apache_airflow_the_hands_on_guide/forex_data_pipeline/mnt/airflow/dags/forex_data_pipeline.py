from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from datetime import datetime, timedelta
import csv
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "orh.dev1@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=4)
}

with DAG(
        "forex_data_pipeline",
        start_date=datetime(2023, 9, 15),
        schedule_interval="@daily",
        default_args=default_args, catchup=False) as dag:
    is_forex_rates_available_task = HttpSensor(
        task_id="is_forex_rates_available_task",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda responses: "rates" in responses.text,
        poke_interval=5,
        timeout=25
    )
