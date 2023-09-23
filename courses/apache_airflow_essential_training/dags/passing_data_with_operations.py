# Importing necessary libraries
import json

# Importing utilities and classes from Apache Airflow
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}


# Function to fetch order prices
def get_order_prices(**kwargs):
    ti = kwargs["ti"]

    # Sample order price data
    order_price_data = {
        'order_1': 237.45,
        'order_2': 10.00,
        'order_3': 33.77,
        'order_4': 44.66,
        'order_5': 377
    }

    # Convert the order price data to a JSON string
    order_price_data_string = json.dumps(order_price_data)
    # Push the JSON string to XCom for inter-task communication
    ti.xcom_push('order_price_data', order_price_data_string)


# Function to compute the sum of order prices
def compute_sum(**kwargs):
    ti = kwargs["ti"]

    # Pull the order price data from XCom
    order_price_data_string = ti.xcom_pull(
        task_ids='get_order_prices', key='order_price_data'
    )

    print(order_price_data_string)

    # Convert the JSON string back to a dictionary
    order_price_data = json.loads(order_price_data_string)

    # Compute the total sum of order prices
    total = sum(order_price_data.values())

    # Push the total sum to XCom
    ti.xcom_push('total_price', total)


# Function to compute the average of order prices
def compute_average(**kwargs):
    ti = kwargs["ti"]

    # Pull the order price data from XCom
    order_price_data_string = ti.xcom_pull(
        task_ids='get_order_prices', key='order_price_data'
    )

    print(order_price_data_string)

    # Convert the JSON string back to a dictionary
    order_price_data = json.loads(order_price_data_string)

    # Compute the average of order prices
    average = sum(order_price_data.values()) / len(order_price_data)

    # Push the average to XCom
    ti.xcom_push('average_price', average)


# Function to display the computed results
def display_result(**kwargs):
    ti = kwargs["ti"]

    # Pull the total and average values from XCom
    total = ti.xcom_pull(task_ids='compute_sum', key='total_price')
    average = ti.xcom_pull(task_ids='compute_average', key='average_price')

    # Print the results
    print(f"Total price of goods {total}")
    print(f"Average price of goods {average}")


# Define the DAG
with DAG(
        dag_id='cross_tasks_communication',
        description='XCom with operators',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['xcom', 'python', 'operators']
) as dag:
    # Define the tasks using PythonOperator
    get_order_prices = PythonOperator(
        task_id='get_order_prices',
        python_callable=get_order_prices
    )

    compute_sum = PythonOperator(
        task_id='compute_sum',
        python_callable=compute_sum
    )

    compute_average = PythonOperator(
        task_id='compute_average',
        python_callable=compute_average
    )

    display_result = PythonOperator(
        task_id='display_result',
        python_callable=display_result
    )

# Define the task dependencies
# First, get_order_prices runs, then compute_sum and compute_average run in parallel,
# and finally, display_result runs.
get_order_prices >> [compute_sum, compute_average] >> display_result
