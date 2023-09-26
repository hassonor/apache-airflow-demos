# Import necessary libraries
import pandas as pd
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

# Set default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}

# Define paths for input and output data
DATASETS_PATH = '/opt/airflow/dags/datasets/car_data.csv'
OUTPUT_PATH = '/opt/airflow/dags/output/{0}.csv'


# Define a function to read the CSV file
def read_csv_file():
    df = pd.read_csv(DATASETS_PATH)
    print(df)
    return df.to_json()


# Define a function to determine which transformation branch to take
# Set a default value for the Airflow Variable "transform" to be 'filter_two_seaters_task'
def determine_branch():
    final_output = Variable.get("transform", default_var='filter_two_seaters')
    if final_output == 'filter_two_seaters':
        return 'filter_two_seaters_task'
    elif final_output == 'filter_fwds':
        return 'filter_fwds_task'


# Define a function to filter two-seater cars
def filter_two_seaters(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file_task')
    df = pd.read_json(json_data)
    two_seater_df = df[df['Seats'] == 2]
    ti.xcom_push(key='transform_result', value=two_seater_df.to_json())
    ti.xcom_push(key='transform_filename', value='two_seaters')


# Define a function to filter cars with FWD powertrain
def filter_fwds(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file_task')
    df = pd.read_json(json_data)
    fwd_df = df[df['PowerTrain'] == 'FWD']
    ti.xcom_push(key='transform_result', value=fwd_df.to_json())
    ti.xcom_push(key='transform_filename', value='fwds')


# Define a function to write the transformed data to a CSV file
def write_csv_result(ti):
    json_data = ti.xcom_pull(key='transform_result')
    file_name = ti.xcom_pull(key='transform_filename')
    df = pd.read_json(json_data)
    df.to_csv(OUTPUT_PATH.format(file_name), index=False)


# Define the DAG
with DAG(
        dag_id='branching_using_operators_v1',
        description='Branching using operators',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['branching', 'python', 'operators']
) as dag:
    # Define tasks within the DAG
    read_csv_file_task = PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file
    )

    determine_branch_task = BranchPythonOperator(
        task_id='determine_branch_task',
        python_callable=determine_branch
    )

    filter_two_seaters_task = PythonOperator(
        task_id='filter_two_seaters_task',
        python_callable=filter_two_seaters
    )

    filter_fwds_task = PythonOperator(
        task_id='filter_fwds_task',
        python_callable=filter_fwds
    )

    write_csv_result_task = PythonOperator(
        task_id='write_csv_result_task',
        python_callable=write_csv_result,
        trigger_rule='none_failed'
    )

# Define the task dependencies
read_csv_file_task >> determine_branch_task >> \
[filter_two_seaters_task, filter_fwds_task] >> write_csv_result_task
