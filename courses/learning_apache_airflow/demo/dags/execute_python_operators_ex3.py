# Import standard Python modules for working with dates and times
from datetime import datetime, timedelta

# Import utility functions from Airflow for working with relative dates (e.g., "days ago")
from airflow.utils.dates import days_ago

# Import the core DAG object from Airflow
from airflow import DAG

# Import the PythonOperator to allow executing Python functions as tasks within Airflow
from airflow.operators.python import PythonOperator

# Define the default arguments for the DAG.
# Here, we're setting the 'owner' attribute for tasks within this DAG.
default_args = {
    'owner': 'orhasson'
}


# Define a Python function to greet someone with a "Hello"
def greet_hello(name):
    print(f"Hello, {name}!")


# Extend the greeting to include the person's city
def greet_hello_with_city(name, city):
    print(f"Hello, {name} from {city}!")


# Define the DAG using a context manager (`with` statement).
with DAG(
        # Assign a unique identifier for the DAG
        dag_id='execute_python_operators_ex3',
        # Provide a description for the DAG
        description='Python operators in DAGs',
        # Assign the default arguments to tasks within this DAG
        default_args=default_args,
        # Define the start date for the DAG. Here, it's set to "1 day ago".
        start_date=days_ago(1),
        # Set the execution frequency. '@daily' implies that this DAG will run once every day.
        schedule_interval='@daily',
        # Add tags for easier categorization and search within the Airflow UI
        tags=['parameter', 'python']
) as dag:
    # Define the first task using PythonOperator. This task will greet with just the name.
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=greet_hello,
        # Use the `op_kwargs` argument to pass keyword arguments to the callable
        op_kwargs={'name': 'Or Hasson'}
    )

    # Define the second task. This task will greet with both name and city.
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=greet_hello_with_city,
        op_kwargs={'name': 'Or Hasson', 'city': 'Tel-Aviv'}
    )

# Set the execution order: task_a will be executed before task_b
task_a >> task_b
