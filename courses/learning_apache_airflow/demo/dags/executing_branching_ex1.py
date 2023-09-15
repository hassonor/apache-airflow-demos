from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

default_args = {
    'owner': 'orhasson',
}


def has_driving_license():
    return choice([True, False])


def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license_task'):
        return 'eligible_to_drive_task'
    else:
        return 'not_eligible_to_drive_task'


def eligible_to_drive():
    print("You can drive, you have a license!")


def not_eligible_to_drive():
    print("I'm afraid you are out of luck, you need a license to drive")


with DAG(
        dag_id='executing_branching_ex1',
        description='Running branching pipelines',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['branching', 'conditions']
) as dag:
    has_driving_license_task = PythonOperator(
        task_id='has_driving_license_task',
        python_callable=has_driving_license
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch
    )

    eligible_to_drive_task = PythonOperator(
        task_id='eligible_to_drive_task',
        python_callable=eligible_to_drive
    )

    not_eligible_to_drive_task = PythonOperator(
        task_id='not_eligible_to_drive_task',
        python_callable=not_eligible_to_drive
    )

has_driving_license_task >> branch_task >> [eligible_to_drive_task, not_eligible_to_drive_task]
