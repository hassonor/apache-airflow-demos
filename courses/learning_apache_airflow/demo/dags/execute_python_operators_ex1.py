# Import necessary modules for working with dates and times
from datetime import datetime, timedelta
# Import utility functions for working with dates in Airflow
from airflow.utils.dates import days_ago

# Import Airflow's core DAG object
from airflow import DAG
# Import the PythonOperator which allows execution of Python callables within Airflow
from airflow.operators.python import PythonOperator

# Set the default arguments for the DAG. In this case, we're only setting the owner of the DAG.
default_args = {
    'owner': 'orhasson'
}


# Define a simple function to print a message
def print_function():
    print("Python operator is so cool!")


# Define the DAG using a context manager (`with` statement)
with DAG(
        # Unique identifier for the DAG
        dag_id='execute_python_operators',
        # A description for the DAG
        description='Python operators in DAGs',
        # Default arguments for the tasks in this DAG
        default_args=default_args,
        # Start date for the DAG. In this case, we're using a utility function to specify "1 day ago".
        start_date=days_ago(1),
        # How often the DAG should run. '@daily' means it will run once every day.
        schedule_interval='@daily',
        # Tags to help with categorizing and searching for this DAG
        tags=['python', 'operator']
) as dag:
    # Define a task using the PythonOperator
    task_1 = PythonOperator(
        # Unique identifier for this task within the DAG
        task_id='task_1',
        # The Python function this task should execute
        python_callable=print_function
    )

# Not part of the DAG definition, but might be a debugging statement or a means to retrieve a reference to the task.
task_1
