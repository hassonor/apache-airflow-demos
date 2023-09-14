import pandas as pd
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Default arguments to be used by the DAG
default_args = {
    'owner': 'orhasson'
}


# Function to read a CSV file, print its contents, and return the data as a JSON string.
# Accepts a filepath as an argument for better flexibility.
def read_csv_file(file_path):
    """
    Read a CSV file and return its contents as JSON.

    Args:
    - file_path (str): Path to the CSV file.

    Returns:
    - str: JSON representation of the DataFrame.
    """
    df = pd.read_csv(file_path)
    print(df)
    return df.to_json()


# Function to remove any rows with null values from a DataFrame.
# Uses XComs to retrieve the DataFrame from a previous task.
def remove_null_values(**kwargs):
    """
    Remove rows with null values from the DataFrame and return the cleaned data as JSON.

    Args:
    - kwargs (dict): Contains runtime keyword arguments.

    Returns:
    - str: JSON representation of the cleaned DataFrame.
    """
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    return df.to_json()


# Define the DAG
with DAG(
        dag_id='executing_python_pipeline_ex1',
        description='Running a Python pipeline',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'transform', 'pipeline']
) as dag:
    # Task to read the CSV file and convert it to JSON
    read_csv_file_task = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file,
        op_args=['/opt/airflow/dags/datasets/insurance.csv']
    )

    # Task to remove rows with null values from the DataFrame
    remove_null_values_task = PythonOperator(
        task_id='remove_null_values_task',
        python_callable=remove_null_values,
    )

# Set the task execution order
read_csv_file_task >> remove_null_values_task
