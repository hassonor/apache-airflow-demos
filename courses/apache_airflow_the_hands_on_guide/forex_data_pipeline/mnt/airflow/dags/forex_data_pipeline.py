from datetime import datetime, timedelta
from utils.create_connections import create_or_verify_http_conn, create_or_verify_file_conn, create_or_verify_hive_conn, \
    create_or_verify_spark_conn, create_or_verify_slack_conn
from utils.data_download import download_rates

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "orh.dev1@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=4)
}


def _get_message():
    return "Hi from Or Hasson"


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

    create_or_verify_spark_conn_task = PythonOperator(
        task_id="create_or_verify_spark_conn_task",
        python_callable=create_or_verify_spark_conn,
    )

    create_or_verify_slack_conn_task = PythonOperator(
        task_id="create_or_verify_slack_conn_task",
        python_callable=create_or_verify_slack_conn,
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

    forex_processing_with_spark = SparkSubmitOperator(
        task_id="forex_processing_with_spark",
        conn_id="spark_conn",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        verbose=False
    )

    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="hassonor@gmail.com",
        subject="forex_data_pipeline",
        html_content="<h4>forex_data_pipeline</h4>"
    )

    send_slack_notification_task = SlackWebhookOperator(
        task_id="send_slack_notification_task",
        http_conn_id="slack_conn",
        message=_get_message(),
        channel="#monitoring"
    )

    [
        create_or_verify_http_conn_task,
        create_or_verify_file_conn_task,
        create_or_verify_hive_conn_task,
        create_or_verify_spark_conn_task,
        create_or_verify_slack_conn_task
    ] >> is_forex_rates_available_task

    is_forex_rates_available_task >> is_forex_currencies_file_available_task

    is_forex_currencies_file_available_task >> download_rates_task

    download_rates_task >> saving_rates_json_on_hdfs_task

    saving_rates_json_on_hdfs_task >> creating_forex_rates_table_on_hive_task

    creating_forex_rates_table_on_hive_task >> forex_processing_with_spark >> [send_email_task,
                                                                               send_slack_notification_task]

# DAG Flow:

# +----------------------------------------+
# | create_or_verify_http_conn_task        |
# | create_or_verify_file_conn_task        |
# | create_or_verify_hive_conn_task        |
# | create_or_verify_spark_conn_task       |
# | create_or_verify_slack_conn_task       |
# +----------------------------------------+
#                     |
#                     v
# +----------------------------------------+
# | is_forex_rates_available_task          |
# +----------------------------------------+
#                     |
#                     v
# +----------------------------------------+
# | is_forex_currencies_file_available_task|
# +----------------------------------------+
#                     |
#                     v
# +----------------------------------------+
# | download_rates_task                    |
# +----------------------------------------+
#                     |
#                     v
# +----------------------------------------+
# | saving_rates_json_on_hdfs_task         |
# +----------------------------------------+
#                     |
#                     v
# +----------------------------------------+
# | creating_forex_rates_table_on_hive_task|
# +----------------------------------------+
#                     |
#                     v
# +----------------------------------------+
# | forex_processing_with_spark            |
# +----------------------------------------+
#                     |
#           +---------+---------------------------------------------+
#           |                                                       |
#           v                                                       V
# +----------------------------------------+    +----------------------------------------+
# | send_email_task                        |    | send_slack_notification_task           |
# +----------------------------------------+    +----------------------------------------+
