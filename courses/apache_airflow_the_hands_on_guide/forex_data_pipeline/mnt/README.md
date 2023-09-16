### Test Task

1. `docker ps`
2. `docker exec -it [airflow Container ID] /bin/bash`
3. `airflow tasks test [DAG_ID] [TASK_ID] 2023-09-16`

### Forex pipeline dag

1. With Dag Run conf
   params: `airflow trigger_dag 'forex_data_pipeline' --conf '{"host":"some_other_host", "endpoint":"some_other_endpoint" "path": "somePathOfFile"}'`
2. Without Dag Run conf
   params: `airflow trigger_dag 'forex_data_pipeline'`
