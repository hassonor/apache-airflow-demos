from airflow import settings
from airflow.models import Connection
import json

# Constants
DEFAULT_HIVE = {
    'CONN_ID': 'hive_conn',
    'HOST': 'hive-server',
    'LOGIN': 'hive',
    'PASSWORD': 'hive',
    'PORT': 10000
}
DEFAULT_HTTP = {'CONN_ID': 'forex_api_conn', 'HOST': 'https://gist.github.com/'}
DEFAULT_FILE = {'CONN_ID': 'forex_path_conn', 'PATH': '/opt/airflow/dags/files'}
DEFAULT_SPARK = {'CONN_ID': 'spark_conn', 'HOST': 'spark://spark-master', 'PORT': 7077}
DEFAULT_SLACK = {'CONN_ID': 'slack_conn', 'HOST': 'https://hooks.slack.com/services/',
                 'PASSWORD': 'TheRestOfTheSlackHost'}


def get_connection_parameters(conn_type, default_values, **kwargs):
    ti = kwargs.get('ti')
    dag_run_conf = ti.dag_run.conf if ti and ti.dag_run else {}
    return {key.lower(): dag_run_conf.get(f'{conn_type}_{key.lower()}', value) for key, value in default_values.items()}


def create_or_verify_connection(conn_type, params):
    with settings.Session() as session:
        existing_conn = session.query(Connection).filter(Connection.conn_id == params['conn_id']).first()
        if not existing_conn:
            conn_id = params.pop('conn_id')
            if 'extra' in params:
                extra = params.pop('extra')
                new_conn = Connection(conn_id=conn_id, conn_type=conn_type, extra=json.dumps(extra), **params)
            else:
                new_conn = Connection(conn_id=conn_id, conn_type=conn_type, **params)
            session.add(new_conn)
            session.commit()


def create_or_verify_hive_conn(**kwargs):
    params = get_connection_parameters('hive_conn', DEFAULT_HIVE, **kwargs)
    create_or_verify_connection("hiveserver2", params)


def create_or_verify_http_conn(**kwargs):
    params = get_connection_parameters('http_conn', DEFAULT_HTTP, **kwargs)
    create_or_verify_connection("http", params)


def create_or_verify_file_conn(**kwargs):
    params = get_connection_parameters('file_conn', DEFAULT_FILE, **kwargs)
    extra_data = {"path": params.pop('path')}
    params['extra'] = extra_data
    create_or_verify_connection("File (path)", params)


def create_or_verify_spark_conn(**kwargs):
    params = get_connection_parameters('spark_conn', DEFAULT_SPARK, **kwargs)
    create_or_verify_connection("spark", params)


def create_or_verify_slack_conn(**kwargs):
    params = get_connection_parameters('slack_conn', DEFAULT_SLACK, **kwargs)
    create_or_verify_connection("http", params)
