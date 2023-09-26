from airflow import settings
from airflow.models import Connection
import json

# Constants
DEFAULT_SQLITE = {
    'CONN_ID': 'my_sqlite_conn',
    'HOST': '/opt/airflow/dags/database/test.db',
}


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


def create_or_verify_sqlite_conn(**kwargs):
    params = get_connection_parameters('sqlite', DEFAULT_SQLITE, **kwargs)
    create_or_verify_connection("sqlite", params)
