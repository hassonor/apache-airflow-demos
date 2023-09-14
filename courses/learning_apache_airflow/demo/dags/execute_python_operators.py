from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'orhasson'
}


def print_function():
    print("Python operator is so cool!")


with DAG(
        dag_id='execute_python_operators',
        description='Python operators in DAGs',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['python', 'operator']
) as dag:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=print_function
    )

task_1
