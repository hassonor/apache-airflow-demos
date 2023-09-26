import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator

from airflow.utils.dates import days_ago
from utils.create_connections import create_or_verify_sqlite_conn

default_args = {
    'owner': 'orhasson'
}

DATASETS_PATH = '/opt/airflow/dags/datasets/car_data.csv'
OUTPUT_PATH = '/opt/airflow/dags/output/{0}.csv'


def create_or_verify_sqlite_connection_func():
    create_or_verify_sqlite_conn()


def read_dataset_func():
    df = pd.read_csv(DATASETS_PATH)
    return df.to_json()


def create_table_func():
    sqlite_operator = SqliteOperator(
        task_id="create_table",
        sqlite_conn_id="my_sqlite_conn",
        sql="""CREATE TABLE IF NOT EXISTS car_data (
                    id INTEGER PRIMARY KEY,
                    brand TEXT NOT NULL,
                    model TEXT NOT NULL,
                    body_style TEXT NOT NULL,
                    seat INTEGER NOT NULL,
                    price INTEGER NOT NULL);""",
        dag=dag
    )
    sqlite_operator.execute(context=None)


def insert_selected_data_func(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='read_dataset_task')
    df = pd.read_json(json_data)
    df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    insert_query = """
        INSERT INTO car_data (brand, model, body_style, seat, price)
        VALUES (?, ?, ?, ?, ?)
    """
    parameters = df.to_dict(orient='records')
    for index, record in enumerate(parameters):
        unique_task_id = f"insert_data_{index}"
        sqlite_operator = SqliteOperator(
            task_id=unique_task_id,
            sqlite_conn_id="my_sqlite_conn",
            sql=insert_query,
            parameters=tuple(record.values()),
            dag=dag
        )
        sqlite_operator.execute(context=None)


dag = DAG(
    'data_transformation_storage_pipeline',
    description='Data transformation and storage pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['sqlite', 'python']
)

create_or_verify_sqlite_connection_task = PythonOperator(
    task_id='create_or_verify_sqlite_connection_task',
    python_callable=create_or_verify_sqlite_connection_func,
    dag=dag
)

read_dataset_task = PythonOperator(
    task_id='read_dataset_task',
    python_callable=read_dataset_func,
    dag=dag
)

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table_func,
    dag=dag
)

insert_selected_data_task = PythonOperator(
    task_id='insert_selected_data_task',
    python_callable=insert_selected_data_func,
    provide_context=True,
    dag=dag
)

create_or_verify_sqlite_connection_task >> read_dataset_task >> create_table_task >> insert_selected_data_task
