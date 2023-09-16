import csv
import json
import requests
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Connection


def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/hassonor/6213b86d299beb5ea67ed5d753146f7f/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')


def create_or_verify_http_conn(conn_id, host):
    """
    Create or verify an HTTP connection.
    """
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    session.close()

    if not existing_conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="HTTP",
            host=host
        )
        session = settings.Session()
        session.add(new_conn)
        session.commit()
        session.close()


def create_or_verify_file_conn(conn_id, path):
    """
    Create or verify a File connection.
    """
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    session.close()

    if not existing_conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="File (path)",
            extra=json.dumps({"path": path})
        )
        session = settings.Session()
        session.add(new_conn)
        session.commit()
        session.close()


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
        template_searchpath="/opt/airflow/dags/bash_scripts/",
        catchup=False
) as dag:
    create_or_verify_http_conn_task = PythonOperator(
        task_id="create_or_verify_http_conn_task",
        python_callable=create_or_verify_http_conn,
        op_args=[
            'forex_api_conn',
            "{{ (dag_run and dag_run.conf and dag_run.conf['host']) or 'https://gist.github.com/' }}"
        ],
        dag=dag
    )

    create_or_verify_file_conn_task = PythonOperator(
        task_id="create_or_verify_file_conn_task",
        python_callable=create_or_verify_file_conn,
        op_args=[
            'forex_path_conn',
            "{{ (dag_run and dag_run.conf and dag_run.conf['path']) or '/opt/airflow/dags/files' }}"
        ],
        dag=dag
    )

    is_forex_rates_available_task = HttpSensor(
        task_id="is_forex_rates_available_task",
        http_conn_id="forex_api_conn",
        endpoint="{{ (dag_run and dag_run.conf and dag_run.conf['endpoint']) or 'hassonor/6213b86d299beb5ea67ed5d753146f7f' }}",
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

    [create_or_verify_http_conn_task,
     create_or_verify_file_conn_task] >> is_forex_rates_available_task >> download_rates_task >> saving_rates_json_on_hdfs_task
