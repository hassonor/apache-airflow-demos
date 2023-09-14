import pandas as pd
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}


# Function to read a CSV file and print its contents.
# The function now takes a file_path argument for flexibility.
def read_csv_file(file_path):
    """
    Read a CSV file from the given path and print its contents.
    Args:
    - file_path (str): Path to the CSV file to read.

    Returns:
    - str: JSON representation of the DataFrame.
    """
    df = pd.read_csv(file_path)
    print(df)
    return df.to_json()


# DAG definition
with DAG(
        dag_id='executing_python_pipeline_ex1',
        description='Running a Python pipeline',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'transform', 'pipeline']
) as dag:
    # PythonOperator to execute the read_csv_file function
    # Now we pass the filepath as an argument to python_callable.
    read_csv_file_task = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file,
        op_args=['/opt/airflow/dags/datasets/insurance.csv']
    )

# Task setting (though typically you'd have more tasks or dependencies)
read_csv_file_task
