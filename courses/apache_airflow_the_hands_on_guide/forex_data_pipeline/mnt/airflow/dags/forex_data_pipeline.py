from datetime import datetime, timedelta
from utils.create_connections import create_or_verify_http_conn, create_or_verify_file_conn, create_or_verify_hive_conn
from utils.data_download import download_rates

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

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
        default_args=default_args,
        template_searchpath="/opt/airflow/dags/scripts/bash_scripts/",
        catchup=False
) as dag:
    create_or_verify_http_conn_task = PythonOperator(
        task_id="create_or_verify_http_conn_task",
        python_callable=create_or_verify_http_conn,
    )

    create_or_verify_file_conn_task = PythonOperator(
        task_id="create_or_verify_file_conn_task",
        python_callable=create_or_verify_file_conn,
    )

    create_or_verify_hive_conn_task = PythonOperator(
        task_id="create_or_verify_hive_conn_task",
        python_callable=create_or_verify_hive_conn,
    )

    is_forex_rates_available_task = HttpSensor(
        task_id="is_forex_rates_available_task",
        http_conn_id="forex_api_conn",
        endpoint="{{ (dag_run and dag_run.conf and dag_run.conf['endpoint']) or "
                 "'hassonor/6213b86d299beb5ea67ed5d753146f7f' }}",
        response_check=lambda responses: "rates" in responses.text,
        poke_interval=5,
        timeout=25
    )

    is_forex_currencies_file_available_task = FileSensor(
        task_id="is_forex_currencies_file_available_task",
        fs_conn_id="forex_path_conn",
        filepath='forex_currencies.csv',
        poke_interval=10,
        timeout=25
    )

    download_rates_task = PythonOperator(
        task_id="download_rates_task",
        python_callable=download_rates,
    )

    saving_rates_json_on_hdfs_task = BashOperator(
        task_id="saving_rates_json_on_hdfs_task",
        bash_command='save_rates_on_hdfs.sh'
    )

    creating_forex_rates_table_on_hive_task = HiveOperator(
        task_id="creating_forex_rates_table_on_hive_task",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                ils DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    [create_or_verify_http_conn_task,
     create_or_verify_file_conn_task,
     create_or_verify_hive_conn_task] >> is_forex_rates_available_task >> download_rates_task >> saving_rates_json_on_hdfs_task >> creating_forex_rates_table_on_hive_task
