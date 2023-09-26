# Import necessary libraries
import pandas as pd
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}

# Paths for input and output data
DATASETS_PATH = '/opt/airflow/dags/datasets/car_data.csv'
OUTPUT_PATH = '/opt/airflow/dags/output/tesla.csv'


# Define the DAG using the decorator
@dag(
    dag_id='interoperating_with_taskflow',
    description='Interoperating traditional tasks with taskflow',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['interop', 'python', 'taskflow', 'operators']
)
def interoperating_with_taskflow():
    # Python function to read the CSV file and push its content to XCom
    def read_csv_file(ti):
        df = pd.read_csv(DATASETS_PATH)
        ti.xcom_push(key='original_data', value=df.to_json())

    # TaskFlow task to filter rows where the Brand is 'Tesla'
    @task
    def filter_tesla(json_data: str):
        df = pd.read_json(json_data)
        tesla_df = df[df['Brand'].str.strip() == 'Tesla']
        return tesla_df.to_json()

    # Python function to write the filtered data to a CSV file
    def write_csv_result(filtered_tesla_json):
        df = pd.read_json(filtered_tesla_json)
        df.to_csv(OUTPUT_PATH, index=False)

    # Define the task to read the CSV using the PythonOperator
    read_csv_file_task = PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file
    )

    # Use the TaskFlow task to filter the data
    filtered_tesla_json = filter_tesla(read_csv_file_task.output['original_data'])

    # Define the task to write the filtered data to a CSV using the PythonOperator
    write_csv_result_task = PythonOperator(
        task_id='write_csv_result_task',
        python_callable=write_csv_result,
        op_args=[filtered_tesla_json]
    )

    # Define the task dependencies
    read_csv_file_task >> filtered_tesla_json >> write_csv_result_task


# Assign the DAG to a variable to make it discoverable by Airflow
dag_instance = interoperating_with_taskflow()
