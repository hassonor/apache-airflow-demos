import pandas as pd
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}


# Function to read CSV file and return its content as JSON
def read_csv_file(file_path):
    df = pd.read_csv(file_path)
    print(df)
    return df.to_json()


# Function to remove null values from a dataframe
def remove_null_values(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='read_csv_file_task')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    return df.to_json()


# Function to group the dataframe by a given column and save the result to a CSV file.
# The filename will include the DAG execution date.
def group_by_and_save(column, output_dir, **kwargs):
    ti = kwargs['ti']
    execution_date = kwargs['execution_date'].strftime('%Y%m%d_%H%M%S')
    json_data = ti.xcom_pull(task_ids='remove_null_values_task')
    df = pd.read_json(json_data)

    grouped_df = df.groupby(column).agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    output_path = f"{output_dir}/grouped_by_{column}_{execution_date}.csv"
    grouped_df.to_csv(output_path, index=False)


# Define the DAG
with DAG(
        dag_id='executing_python_pipeline_ex1',
        description='Running a Python pipeline',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'transform', 'pipeline']
) as dag:
    # Task to read the CSV file
    read_csv_file_task = PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file,
        op_args=['/opt/airflow/dags/datasets/insurance.csv']
    )

    # Task to remove null values from the dataframe
    remove_null_values_task = PythonOperator(
        task_id='remove_null_values_task',
        python_callable=remove_null_values,
        provide_context=True
    )

    # Task to group by the "smoker" column and save the result
    group_by_smoker_task = PythonOperator(
        task_id="group_by_smoker_task",
        python_callable=group_by_and_save,
        op_args=['smoker', '/opt/airflow/dags/output'],
        provide_context=True  # Provides execution_date to the function
    )

    # Task to group by the "region" column and save the result
    group_by_region_task = PythonOperator(
        task_id="group_by_region_task",
        python_callable=group_by_and_save,
        op_args=['region', '/opt/airflow/dags/output'],
        provide_context=True  # Provides execution_date to the function
    )

    # Set the task execution order
    read_csv_file_task >> remove_null_values_task >> [group_by_smoker_task, group_by_region_task]

# read_csv_file_task
#        |
#        V
# remove_null_values_task
#        |
#        |-------------------------|
#        |                        |
#        V                        V
# group_by_smoker_task   group_by_region_task
