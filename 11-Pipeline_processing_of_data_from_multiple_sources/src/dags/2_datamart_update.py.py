# Обновление витрины. Соединение vertica_connect c указанием параметров подключения создано в Airflow
# --------------------------------------------------------------------------------------------

import pandas as pd

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Имя соединения созданного в UI Airflow
vertica_conn_id = 'vertica_connect'

with DAG(
        'update_datamart',
        default_args={"retries": 1},
        description='Provide default dag for sprint3',
        catchup=False,
        start_date=datetime(2022, 10, 1),
        schedule_interval='0 5 * * *',
        max_active_runs = 1
) as dag:
        update_global_datamart = VerticaOperator(
        task_id='update_global_datamart',
        vertica_conn_id=vertica_conn_id,
        sql="src\sql\update_datamart.sql") 


update_global_datamart
