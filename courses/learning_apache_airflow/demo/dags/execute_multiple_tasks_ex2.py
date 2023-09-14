from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}

# Start defining the DAG
with DAG(
        dag_id='executing_multiple_tasks_ex2',
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
        bash_command='task_b1.sh'
    )

    # Define task_c which will execute task_c.sh
    task_c = BashOperator(
        task_id='task_c',
        bash_command='task_c1.sh'
    )

    # Define task_d which will execute task_d.sh
    task_d = BashOperator(
        task_id='task_d',
        bash_command='task_d.sh'
    )

# Define task dependencies using different ways:

# Way 1: task_a triggers both task_b and task_c. Once both task_b and task_c finish, task_d triggers.
task_a >> [task_b, task_c]
task_d << [task_b, task_c]

# Way 2 (Alternative method to Way 1):
# task_a >> task_b
# task_a >> task_c
# task_d << task_b
# task_d << task_c

# Way 3 (Another alternative method):
# a --> b,c (b and c will trigger only if a succeeded)
# task_a.set_downstream(task_b)
# task_a.set_downstream(task_c)
# b,c --> d (d will trigger only if b and c succeeded)
# task_d.set_upstream(task_b)
# task_d.set_upstream(task_c)

# Flow:
#
#    task_a
#    /     \
#  task_b  task_c
#    \     /
#    task_d
