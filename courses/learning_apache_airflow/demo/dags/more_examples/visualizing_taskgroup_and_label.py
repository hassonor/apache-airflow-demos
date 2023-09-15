import pandas as pd
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args = {
    'owner': 'orhasson'
}


def read_csv_file(ti):
    df = pd.read_csv('/Users/orhasson/airflow/datasets/insurance.csv')

    print(df)

    ti.xcom_push(key='my_csv', value=df.to_json())


def remove_null_values(ti):
    json_data = ti.xcom_pull(key='my_csv')

    df = pd.read_json(json_data)

    df = df.dropna()

    print(df)

    ti.xcom_push(key='my_clean_csv', value=df.to_json())


def filter_by_region(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']

    region_df.to_csv('/Users/orhasson/airflow/output/filtered_by_region.csv', index=False)


def filter_bmi_smoker_charges(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    selected_cols_df = df[['bmi', 'smoker', 'charges']]

    selected_cols_df.to_csv('/Users/orhasson/airflow/output/selected_cols.csv', index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv('/Users/orhasson/airflow/output/grouped_by_region.csv', index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv('/Users/orhasson/airflow/output/grouped_by_smoker.csv', index=False)


def determine_branch():
    final_output = Variable.get("final_output", default_var=None)

    if final_output == 'filter_by_region':
        return 'Filtering.filter_by_region'
    elif final_output == 'filter_bmi_smoker_charges':
        return 'Filtering.filter_bmi_smoker_charges'
    else:
        return 'Grouping.groupby_region_smoker'


with DAG(
        dag_id='taskgroup_and_labels_pipeline',
        description='Running a Branching pipeline',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'transform', 'pipeline', 'branching', 'taskgroup', 'labels']
) as dag:
    with TaskGroup('Reading_and_Preprocessing') as reading_and_preprocessing:
        read_csv_file = PythonOperator(
            task_id='read_csv_file',
            python_callable=read_csv_file
        )

        remove_null_values = PythonOperator(
            task_id='remove_null_values',
            python_callable=remove_null_values
        )

        read_csv_file >> remove_null_values

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    with TaskGroup('Filtering') as filtering:
        filter_by_region = PythonOperator(
            task_id='filter_by_region',
            python_callable=filter_by_region
        )

        filter_bmi_smoker_charges = PythonOperator(
            task_id='filter_bmi_smoker_charges',
            python_callable=filter_bmi_smoker_charges
        )

    with TaskGroup('Grouping') as grouping:
        groupby_region_smoker = PythonOperator(
            task_id='groupby_region_smoker',
            python_callable=groupby_region_smoker
        )

    reading_and_preprocessing >> Label('Clean Data') >> determine_branch >> Label('Based on Variable is Branched') >> [
        filtering, grouping]
