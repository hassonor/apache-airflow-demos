from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'orhasson'
}


def _print_context(**kwargs):
    print(kwargs)


with DAG(
        dag_id='print_context_vars',
        description='Branching using operators',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'operators']) as dag:
    print_context = PythonOperator(
        task_id="print_context", python_callable=_print_context
    )

print_context
