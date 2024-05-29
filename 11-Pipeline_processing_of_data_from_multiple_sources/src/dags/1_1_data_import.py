# Вариант загрузки данных через scv файл с указанием параметров подключения в явном виде
# Не лучшее решение, т.к. параметры подключения видны всем

import contextlib
from typing import Dict, List, Optional
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
 
from airflow.decorators import dag
 
import pandas as pd
import pendulum
import vertica_python
import psycopg2 
 
def load_dataset_file_to_vertica(
    table_name: str,
    schema: str,
    table: str,
    table_rej: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    
    # Создание подключения к PostgreSQL
    conn_src = psycopg2.connect(database = "db1",
                                host =     "rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
                                user =     "student",
                                password = "de_student_112022",
                                port =     "6432",
                                # sslmode=   'require'
                                )
    # Создание подключения к Vertica
    vertica_conn = vertica_python.connect(
                                host='51.250.75.20',
                                port=5433,
                                user='stv2023070314',
                                password= 'wzXQSM7GzUWEVpC',
                                database= 'dwh'
                            )
    # Создание курсора postgresql
    cursor_src = conn_src.cursor()
    # Создание курсора vertica
    cursor_vert = vertica_conn.cursor()

    # Определяем последнюю дату обновления stg таблицы в vertica
    cursor_vert.execute( f''' SELECT date(MAX({column_date}))
						                            FROM STV2023070314__STAGING.{table_name} '''
                                                     )
    max_date = cursor_vert.fetchone()[0]

    # Чтение данных из БД источника src в DataFrame

    cursor_src.execute( f''' SELECT *
                            FROM public.{table_name}
                            WHERE DATE({column_date}) > ( %s ) ''', [max_date] ) 

    # cursor_src.execute( f''' SELECT *
    #                             FROM public.{table_name}
    #                      ''' ) 

    records = cursor_src.fetchall()
    names = [x[0] for x in cursor_src.description]
    df = pd.DataFrame(records, columns = names)

    num_rows = len(df)

    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"' null '' skip 1 rejected data as table {schema}.{table_rej}
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1
 
    vertica_conn.close()
 
 
@dag(schedule_interval='0 5 * * *', 
    start_date=pendulum.parse('2022-10-01'), 
    max_active_runs = 1, 
    default_args={"retries": 1},
    catchup=False)

def project_load_data_to_staging_dag():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    load_transactions = PythonOperator(
        task_id='load_transactions',
        python_callable=load_dataset_file_to_vertica, 
        op_kwargs={
            'table_name': 'transactions',
            'column_date' : 'transaction_dt',
            'schema': 'STV2023070314__STAGING',
            'table': 'transactions',
            'table_rej': 'transactions_rej',
            'columns': ['operation_id','account_number_from' ,'account_number_to' ,'currency_code' ,'country' ,'status' ,'transaction_type' ,'amount' ,'transaction_dt' ],
            # 'type_override': {'user_id_from': 'Int64'},
        },
    )
 

    load_currencies = PythonOperator(
        task_id='load_currencies',
        python_callable=load_dataset_file_to_vertica, 
        op_kwargs={
            'table_name': 'currencies',
            'column_date' : 'date_update',
            'schema': 'STV2023070314__STAGING',
            'table': 'currencies',
            'table_rej': 'currencies_rej',
            'columns': ['date_update','currency_code','currency_code_with','currency_with_div'],
            # 'type_override': {'user_id_from': 'Int64'},
        },
    )


    start >> [load_transactions, load_currencies] >> end
 
 
_ = project_load_data_to_staging_dag()
