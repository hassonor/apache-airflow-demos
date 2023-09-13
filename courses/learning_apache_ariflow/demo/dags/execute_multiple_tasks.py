from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'orhasson'
}

with DAG(
        dag_id='executing_multiple_tasks',
        description='Dag with multiple tasks and depends',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once'
) as dag:
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo task A has executed!'
    )

    task_b = BashOperator(
        task_id='task_b',
        bash_command='echo task A has executed!'
    )

task_a.set_downstream(task_b)  # a --> b
# task_a.set_upstream(task_b)  # b --> a
