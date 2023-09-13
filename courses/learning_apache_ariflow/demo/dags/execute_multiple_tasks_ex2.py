from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'orhasson'
}

with DAG(
        dag_id='executing_multiple_tasks_ex2',
        description='Dag with multiple tasks and depends',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=timedelta(days=1),
        tags=['upstream', 'downstream']
) as dag:
    task_a = BashOperator(
        task_id='task_a',
        bash_command='''
            echo Task A has started!
            
            for i in {1..10}
            do
                echo Task A printing Or Hasson was here $i
            done
            
            echo Task A has ended. B we come!
        '''
    )

    task_b = BashOperator(
        task_id='task_b',
        bash_command='''
            echo Task B has started
            sleep 3
            echo Task B has ended. C we come!
        '''
    )

    task_c = BashOperator(
        task_id='task_c',
        bash_command='''
             echo Task C has started
             sleep 5
             echo Task C has ended. D we come!
         '''
    )

    task_d = BashOperator(
        task_id='task_d',
        bash_command='echo Task D completed. Cheers!'
    )

# Way 1:
task_a >> [task_b, task_c]
task_d << [task_b, task_c]

# Way 2:
# task_a >> task_b
# task_a >> task_c

# task_d << task_b
# task_d << task_c

# Way 3
# a --> b,c (b and c will trigger only if a succeeded)
# task_a.set_downstream(task_b)
# task_a.set_downstream(task_c)

# b,c --> d (d will trigger only if b and c succeeded)
# task_d.set_upstream(task_b)
# task_d.set_upstream(task_c)
