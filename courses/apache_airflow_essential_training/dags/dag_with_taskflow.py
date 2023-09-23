# Importing necessary libraries
import time
from datetime import datetime, timedelta

# Importing utilities and decorators from Apache Airflow
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

# Default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}


# Defining the DAG using the new TaskFlow API
@dag(
    dag_id='dag_with_taskflow',  # Unique identifier for the DAG
    default_args=default_args,  # Default arguments for the DAG
    start_date=days_ago(1),  # Start date for the DAG
    schedule_interval='@once',  # Schedule interval (runs only once in this case)
    tags=['dependencies', 'python', 'taskflow_api']  # Tags for better categorization and filtering
)
def dag_with_taskflow_api():
    # Define task A
    @task
    def task_a():
        print("Task A executed!")

    # Define task B with a sleep duration
    @task
    def task_b():
        time.sleep(5)
        print("Task B executed!")

    # Define task C with a sleep duration
    @task
    def task_c():
        time.sleep(5)
        print("Task C executed!")

    # Define task D with a sleep duration
    @task
    def task_d():
        time.sleep(5)
        print("Task D executed!")

    # Define task E with a sleep duration
    @task
    def task_e():
        time.sleep(5)
        print("Task E executed!")

    # Setting up task dependencies:
    # Task A runs first, then tasks B, C, and D run in parallel,
    # and finally, task E runs after B, C, and D have completed.
    task_a() >> [task_b(), task_c(), task_d()] >> task_e()


# Instantiate the DAG
dag_with_taskflow_api()
