import time
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner': 'orhasson'
}


@dag(dag_id='dag_with_taskflow',
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@once',
     tags=['dependencies', 'python', 'taskflow_api']
     )
def dag_with_taskflow_api():
    @task
    def task_a():
        print("Task A executed!")

    @task
    def task_b():
        time.sleep(5)
        print("Task B executed!")

    @task
    def task_c():
        time.sleep(5)
        print("Task C executed!")

    @task
    def task_d():
        time.sleep(5)
        print("Task D executed!")

    @task
    def task_e():
        time.sleep(5)
        print("Task E executed!")

    task_a() >> [task_b(), task_c(), task_d()] >> task_e()


dag_with_taskflow_api()
