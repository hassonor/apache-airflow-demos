### Test Task

1. `docker ps`
2. `docker exec -it [airflow Container ID] /bin/bash`
3. `airflow tasks test [DAG_ID] [TASK_ID] 2023-09-16`

### Forex pipeline dag

1. With Dag Run conf
   params: `airflow trigger_dag 'forex_data_pipeline' --conf '{"host":"some_other_host", "endpoint":"some_other_endpoint" "path": "somePathOfFile"}'`
2. Without Dag Run conf
   params: `airflow trigger_dag 'forex_data_pipeline'`

### Airflow, Hue, Spark

1. Login Airflow: Navigate `localhost:9091` airflow;airflow
2. Login Hue (To watch and query HDFS data): Navigate to `localhost:32762` root;root
