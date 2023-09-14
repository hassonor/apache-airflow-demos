from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'orhasson'
}

# Start defining the DAG
with DAG(
        dag_id='executing_multiple_tasks_ex1',
        description='Dag with multiple tasks and depends',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once'  # The DAG will run only once and not on a recurring schedule
) as dag:
    # Define task_a which will print "task A has executed!"
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo task A has executed!'
    )

    # Define task_b which will print "task B has executed!"
    task_b = BashOperator(
        task_id='task_b',
        bash_command='echo task B has executed!'
    )

# Define task dependencies:
# Here, task_a is set to trigger before task_b, i.e., a --> b
task_a.set_downstream(task_b)

# Flow:
#
#    task_a
#      ↓
#    task_b

# task_a.set_upstream(task_b)  # This makes b --> a
