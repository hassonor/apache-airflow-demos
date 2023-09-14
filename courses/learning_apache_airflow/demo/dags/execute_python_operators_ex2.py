# Necessary imports for working with dates and times
from datetime import datetime, timedelta
# Utility functions to work with dates in Airflow
from airflow.utils.dates import days_ago

# Importing core Airflow components
from airflow import DAG
# Import the PythonOperator for executing Python functions as tasks
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}


# Define four Python functions, each prints a message when executed
def print_a():
    print("Task A executed")


def print_b():
    print("Task B executed")


def print_c():
    print("Task C executed")


def print_d():
    print("Task D executed")


# Construct the DAG
with DAG(
        # Unique identifier for the DAG
        dag_id='execute_python_operators_ex2',
        # Description for the DAG
        description='Python operators in DAGs',
        # Default arguments for tasks in the DAG
        default_args=default_args,
        # Start date for the DAG; using a utility to denote "1 day ago"
        start_date=days_ago(1),
        # DAG execution frequency; '@daily' means once every day
        schedule_interval='@daily',
        # Tags for the DAG; helps in categorizing/searching
        tags=['python', 'operator']
) as dag:
    # Define four tasks using PythonOperator, each corresponding to one of the functions defined earlier
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=print_a
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=print_b
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=print_c
    )

    task_d = PythonOperator(
        task_id='task_d',
        python_callable=print_d
    )

# Define the relationships between the tasks. This determines execution order.
# After `task_a` completes, both `task_b` and `task_c` will execute in parallel
task_a >> [task_b, task_c]
# Once both `task_b` and `task_c` have completed, `task_d` will execute
[task_b, task_c] >> task_d

#    task_a
#    /    \
# task_b  task_c
#    \    /
#    task_d
