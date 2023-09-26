from airflow.utils.dates import days_ago

from airflow.decorators import dag, task

default_args = {
    'owner': 'orhasson'
}


@dag(
    dag_id='passing_data_with_taskflow_api',
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

    @task(multiple_outputs=True)
    def compute_total_and_average(order_price_data):
        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order]
            count += 1

        average = total / count

        return {'total_price': total, 'average_price': average}

    @task
    def display_result(total, average):
        print(f"Total price of goods {total}")
        print(f"Average price of good {average}")

    order_price_data = get_order_prices()

    total_and_average_dict = compute_total_and_average(order_price_data)

    display_result(total_and_average_dict['total_price'], total_and_average_dict['average_price'])


passing_data_with_taskflow_api()
