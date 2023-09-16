from airflow import settings
from airflow.models import Connection
import json

# Constants
DEFAULT_HOST = 'https://gist.github.com/'
DEFAULT_PATH = '/opt/airflow/dags/files'
DEFAULT_ENDPOINT = 'hassonor/6213b86d299beb5ea67ed5d753146f7f'
DEFAULT_HIVE_HOST = 'hive-server'
DEFAULT_HIVE_LOGIN = 'hive'
DEFAULT_HIVE_PASSWORD = 'hive'
DEFAULT_HIVE_PORT = 10000


def create_or_verify_hive_conn(**kwargs):
    ti = kwargs.get('ti')
    dag_run_conf = ti.dag_run.conf if ti and ti.dag_run else {}

    conn_id = "hive_conn"
    host = dag_run_conf.get('hive_host', DEFAULT_HIVE_HOST)
    login = dag_run_conf.get('hive_login', DEFAULT_HIVE_LOGIN)
    password = dag_run_conf.get('hive_password', DEFAULT_HIVE_PASSWORD)
    port = dag_run_conf.get('hive_port', DEFAULT_HIVE_PORT)

    with settings.Session() as session:
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if not existing_conn:
            new_conn = Connection(
                conn_id="hive_test",
                conn_type="hiveserver2",
                host=host,
                login=login,
                password=password,
                port=port
            )
            session.add(new_conn)
            session.commit()


def create_or_verify_http_conn(**kwargs):
    ti = kwargs.get('ti')
    dag_run_conf = ti.dag_run.conf if ti and ti.dag_run else {}
    conn_id = "forex_api_conn"
    host = dag_run_conf.get('host', DEFAULT_HOST)

    with settings.Session() as session:
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if not existing_conn:
            new_conn = Connection(
                conn_id=conn_id,
                conn_type="HTTP",
                host=host
            )
            session.add(new_conn)
            session.commit()


def create_or_verify_file_conn(**kwargs):
    ti = kwargs.get('ti')
    dag_run_conf = ti.dag_run.conf if ti and ti.dag_run else {}
    conn_id = "forex_path_conn"
    path = dag_run_conf.get('path', DEFAULT_PATH)

    with settings.Session() as session:
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if not existing_conn:
            new_conn = Connection(
                conn_id=conn_id,
                conn_type="File (path)",
                extra=json.dumps({"path": path})
            )
            session.add(new_conn)
            session.commit()
