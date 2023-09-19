# Script to add new DAGs folders using 
# the class DagBag
# Paths must be absolute
import os
from airflow.models import DagBag

dags_dirs = [
    '"$AIRFLOW_HOME"/dags/project_a',
    '"$AIRFLOW_HOME"/dags/project_b'
]

for dir in dags_dirs:
    dag_bag = DagBag(os.path.expanduser(dir))

    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag
