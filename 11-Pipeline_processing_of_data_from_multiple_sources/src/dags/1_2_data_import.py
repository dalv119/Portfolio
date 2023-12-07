# Вариант загрузки данных через датафрейм
# создав подключение в Airflow как pg_connect и vertica_connect и передав их через Hook

from typing import Dict, List, Optional
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


from airflow.decorators import dag
 
import pandas as pd
import pendulum
import vertica_python
import psycopg2 
from datetime import datetime
 
def load_dataset_file_to_vertica(
    table_name: str,
    column_date: datetime,
    schema: str,
    table: str,
    table_rej: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    # Создание подключения к PostgreSql
    src = PostgresHook(postgres_conn_id='pg_connect')
    conn_src = src.get_conn()

    # Создание подключения к Vertica
    dest = VerticaHook(vertica_conn_id='vertica_connect')
    vertica_conn = dest.get_conn()

    # Создание курсора postgresql
    cursor_src = conn_src.cursor()
    # Создание курсора vertica
    cursor_vert = vertica_conn.cursor()

    # Определяем последнюю дату обновления stg таблицы в vertica
    cursor_vert.execute( f''' SELECT date(MAX({column_date}))
						                            FROM STV2023070314__STAGING.{table_name} '''
                                                     )
    max_date = cursor_vert.fetchone()[0]

    # Чтение данных из источника src в DataFrame
    cursor_src.execute( f''' SELECT *
                            FROM public.{table_name}
                            WHERE DATE({column_date}) > ( %s ) ''', [max_date] ) 

    # cursor_src.execute( f''' SELECT *
    #                             FROM public.{table_name}
    #                      ''' ) 

    records = cursor_src.fetchall()
    names = [x[0] for x in cursor_src.description]
    df = pd.DataFrame(records, columns = names)

    tab = table_name

    # Запись DataFrame в stg таблицу базы данных vertica
    if tab == 'transactions':
        cursor_vert.executemany( ''' INSERT INTO STV2023070314__STAGING.transactions(
                                        operation_id ,
                                        account_number_from ,
                                        account_number_to ,
                                        currency_code ,
                                        country ,
                                        status ,
                                        transaction_type ,
                                        amount ,
                                        transaction_dt   
                                    ) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s ) ''', df.values.tolist() )
    elif tab == 'currencies':
        cursor_vert.executemany( ''' INSERT INTO STV2023070314__STAGING.currencies(
                                        date_update ,
                                        currency_code ,
                                        currency_code_with ,
                                        currency_with_div   
                                    ) VALUES( %s, %s, %s, %s ) ''', df.values.tolist() )

    vertica_conn.commit()   
    vertica_conn.close()
    conn_src.close()
 
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