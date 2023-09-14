# Import necessary modules for working with dates and times
from datetime import datetime, timedelta
# Import Airflow's utility functions for working with dates
from airflow.utils.dates import days_ago

# Import the core DAG object from Airflow
from airflow import DAG
# Import PythonOperator to execute Python functions as tasks
from airflow.operators.python import PythonOperator

# Define the default arguments for the DAG; here we set the owner of the DAG
default_args = {
    'owner': 'orhasson'
}


# Function to add an increment to a value
def add_increment(value, increment):
    result = value + increment
    print(f"Initial value: {value}, incremented by: {increment}, result: {result}")
    return result


# Function to multiply a value pulled from another task's XCOM
def multiply(ti, multiplier):
    value = ti.xcom_pull(task_ids='add_increment_task')
    result = value * multiplier
    print(f"Value after multiplication: {result}")
    return result


# Function to subtract a decrement from a value pulled from another task's XCOM
def subtract(ti, decrement):
    value = ti.xcom_pull(task_ids='multiple_task')
    result = value - decrement
    print(f"Value after subtraction: {result}")
    return result


# Function to print a value pulled from another task's XCOM
def print_final_value(ti):
    value = ti.xcom_pull(task_ids='subtract_task')
    print(f"Final value: {value}")


# Define the DAG using a context manager (`with` statement)
with DAG(
        dag_id='cross_task_communication_ex1',
        description='Demonstrate cross-task communication using XCOM in Airflow',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['xcom', 'python']
) as dag:
    add_increment_task = PythonOperator(
        task_id='add_increment_task',
        python_callable=add_increment,
        op_kwargs={'value': 1, 'increment': 1}
    )

    multiple_task = PythonOperator(
        task_id='multiple_task',
        python_callable=multiply,
        op_kwargs={'multiplier': 100}
    )

    subtract_task = PythonOperator(
        task_id='subtract_task',
        python_callable=subtract,
        op_kwargs={'decrement': 9}
    )

    print_value_task = PythonOperator(
        task_id='print_value_task',
        python_callable=print_final_value
    )

# Define the task execution order
add_increment_task >> multiple_task >> subtract_task >> print_value_task
