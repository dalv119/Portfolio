import requests
import json
from psycopg2.extras import execute_values
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

task_logger = logging.getLogger('airflow.task')

# подключение к ресурсам
api_conn = BaseHook.get_connection('api_conn')
postgres_conn = 'PG_WAREHOUSE_CONNECTION' # 'PG_DWH'
dwh_hook = PostgresHook(postgres_conn)

# параметры API
nickname = 'dobrov'
cohort = '14'
api_key = json.loads(api_conn.extra)['api_key'] # или api_key=25c27781-8fde-4b30-a22e-524044a7580f или в соединении Airflow extra -> {"api_key": "25c27781-8fde-4b30-a22e-524044a7580f"}
base_url = api_conn.host


headers = {"X-Nickname" : nickname,
         'X-Cohort' : cohort,
         'X-API-KEY' : api_key,
         }
         


def upload_couriers(pg_schema, pg_table):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    
    # идемпотентность
    dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

    method_url = 'couriers'

    offset = 0

    while True:    
        couriers_rep = requests.get(f'https://{base_url}/{method_url}/?sort_field=_id&sort_direction=asc&offset={offset}',
                            headers = headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        
        columns = ','.join([i for i in couriers_rep[0]])
        values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]

        sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s"
        execute_values(cursor, sql, values)

        offset += len(couriers_rep)  


default_args = {
    'owner':'airflow',
    'retries':1,
    'retry_delay': timedelta (seconds = 60)
}


dag = DAG('stg_delivery_dag',
        start_date=datetime(2022, 10, 15),
        catchup=True,
        schedule_interval='@daily',
        max_active_runs=1,
        default_args=default_args)

with TaskGroup(group_id = 'upload_stg', dag=dag) as upload_stg:


    upload_couriers = PythonOperator(
                task_id = 'stg_couriers',
                python_callable = upload_couriers,
                op_kwargs = {
                    'pg_schema' : 'test_stg',
                    'pg_table' : 'couriers'
                },
                dag = dag
    )




upload_stg