from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 9, 18, 1),
    'owner': 'airflow'
}

with DAG(dag_id='project_a', schedule_interval="0 0 * * *", default_args=default_args, catchup=False) as dag:
    # Task 1
    bash_task_1 = BashOperator(task_id='bash_task_1', bash_command="echo 'first task'")

    # Task 2
    bash_task_2 = BashOperator(task_id='bash_task_2', bash_command="echo 'second task'")

    bash_task_1 >> bash_task_2
