from airflow.utils.dates import days_ago

from airflow.decorators import dag, task

default_args = {
    'owner': 'orhasson'
}


@dag(
    dag_id='passing_data_with_tasks',
    description='XCom using the TaskFlow API',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['xcom', 'python', 'taskflow_api']
)
def passing_data_with_taskflow_api():
    @task
    def get_order_prices(**kwargs):
        order_price_data = {
            'order_1': 237.45,
            'order_2': 10.00,
            'order_3': 33.77,
            'order_4': 44.66,
            'order_5': 377
        }

        return order_price_data

    @task
    def compute_sum(order_price_data):
        total = 0
        for order in order_price_data:
            total += order_price_data[order]

        return total

    @task
    def compute_average(order_price_data):
        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order]
            count += 1

        average = total / count

        return average

    @task
    def display_result(total, average):
        print(f"Total price of goods {total}")
        print(f"Average price of good {average}")

    order_price_data = get_order_prices()

    total = compute_sum(order_price_data)
    average = compute_average(order_price_data)

    display_result(total, average)


passing_data_with_taskflow_api()
