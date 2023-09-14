from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}

# Start defining the DAG
with DAG(
        dag_id='executing_multiple_tasks_ex3',
        description='Dag with multiple tasks and depends',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=timedelta(days=1),
        tags=['scripts', 'template_search'],
        template_searchpath="/opt/airflow/dags/bash_scripts/"
) as dag:
    # Define task_a which will execute task_a.sh
    task_a = BashOperator(
        task_id='task_a',
        bash_command='task_a.sh'
    )

    # Define task_b which will execute task_b.sh
    task_b = BashOperator(
        task_id='task_b',
        bash_command='task_b.sh'
    )

    # Define task_c which will execute task_c.sh
    task_c = BashOperator(
        task_id='task_c',
        bash_command='task_c.sh'
    )

    # Define task_d which will execute task_d.sh
    task_d = BashOperator(
        task_id='task_d',
        bash_command='task_d.sh'
    )

    # Define task_e which will execute task_e.sh
    task_e = BashOperator(
        task_id='task_e',
        bash_command='task_e.sh'
    )

    # Define task_f which will execute task_f.sh
    task_f = BashOperator(
        task_id='task_f',
        bash_command='task_f.sh'
    )

    # Define task_g which will execute task_g.sh
    task_g = BashOperator(
        task_id='task_g',
        bash_command='task_g.sh'
    )

# Define task dependencies

# task_a runs before task_b, which runs before task_e
task_a >> task_b >> task_e

# task_a runs before task_c, which runs before task_f
task_a >> task_c >> task_f

# task_a runs before task_d, which runs before task_g
task_a >> task_d >> task_g

# Flow:
#
#   task_a
#   /  |  \
#  /   |   \
# b    c    d
# |    |    |
# e    f    g
