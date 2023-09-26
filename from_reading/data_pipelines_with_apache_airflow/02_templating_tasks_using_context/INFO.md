### Airflow Task Context Variables

| Key                   | Description                                                | Example                                                                                                      |
|-----------------------|------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `ti`                  | TaskInstance object                                        | `<TaskInstance: my_dag.my_task 2023-09-26 00:00:00+00:00 [success]>`                                         |
| `task`                | Task object                                                | `<Task(PythonOperator): my_task>`                                                                            |
| `ds`                  | Execution date as `YYYY-MM-DD`                             | `2023-09-26`                                                                                                 |
| `ds_nodash`           | Execution date without dashes                              | `20230926`                                                                                                   |
| `next_ds`             | Next execution date as `YYYY-MM-DD`                        | `2023-09-27`                                                                                                 |
| `next_ds_nodash`      | Next execution date without dashes                         | `20230927`                                                                                                   |
| `prev_ds`             | Previous execution date as `YYYY-MM-DD`                    | `2023-09-25`                                                                                                 |
| `prev_ds_nodash`      | Previous execution date without dashes                     | `20230925`                                                                                                   |
| `execution_date`      | Execution date as a datetime object                        | `datetime.datetime(2023, 9, 26, 0, 0)`                                                                       |
| `prev_execution_date` | Previous execution date as a datetime object               | `datetime.datetime(2023, 9, 25, 0, 0)`                                                                       |
| `next_execution_date` | Next execution date as a datetime object                   | `datetime.datetime(2023, 9, 27, 0, 0)`                                                                       |
| `dag`                 | DAG object                                                 | `<DAG: my_dag>`                                                                                              |
| `dag_run`             | DAGRun object                                              | `<DAGRun my_dag @ 2023-09-26 00:00:00+00:00: manual__2023-09-26T00:00:00+00:00, externally triggered: True>` |
| `run_id`              | ID for the current DAG run                                 | `manual__2023-09-26T00:00:00+00:00`                                                                          |
| `conf`                | Configuration passed to the DAG run                        | `{'param1': 'value1', 'param2': 'value2'}`                                                                   |
| `params`              | Parameters defined for the DAG                             | `{'param1': 'value1', 'param2': 'value2'}`                                                                   |
| `test_mode`           | Boolean that is True if the task is being run in test mode | `True/False`                                                                                                 |
| `var`                 | Access to Airflow Variables                                | `{{ var.value.my_var_name }}`                                                                                |
| `task_instance`       | TaskInstance object (same as `ti`)                         | `<TaskInstance: my_dag.my_task 2023-09-26 00:00:00+00:00 [success]>`                                         |
| `inlets`              | Task inlets                                                | `[]`                                                                                                         |
| `outlets`             | Task outlets                                               | `[]`                                                                                                         |
| `macros`              | Access to all available macros                             | `{{ macros.ds_add(ds, 7) }}`                                                                                 |
| `templates_dict`      | Dictionary of templates that will get templated            | `{'my_key': 'my_value'}`                                                                                     |
| `end_date`            | End date for the task                                      | `datetime.datetime(2023, 9, 27, 0, 0)`                                                                       |
| `latest_date`         | Latest date for the task                                   | `datetime.datetime(2023, 9, 27, 0, 0)`                                                                       |
| `yesterday_ds`        | Day before the execution date as `YYYY-MM-DD`              | `2023-09-25`                                                                                                 |
| `yesterday_ds_nodash` | Day before the execution date without dashes               | `20230925`                                                                                                   |
| `tomorrow_ds`         | Day after the execution date as `YYYY-MM-DD`               | `2023-09-27`                                                                                                 |
| `tomorrow_ds_nodash`  | Day after the execution date without dashes                | `20230927`                                                                                                   |

