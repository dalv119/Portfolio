# Обновление витрины. Соединение vertica_connect c указанием параметров подключения в явном виде
# ----------------------------------------------------------------------------------------------

import pandas as pd
import vertica_python
import psycopg2 


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.models import Variable


# vertica_conn_id = 'vertica_connect'
# vertica_conn = VerticaHook.get_conn('vertica_connect')

def datamart_update():
   
    # Создание подключения к Vertica
    vertica_conn = vertica_python.connect(
                                host='51.250.75.20',
                                port=5433,
                                user='stv2023070314',
                                password= 'wzXQSM7GzUWEVpC',
                                database= 'dwh'
                            )
    # Создание курсора
    cursor_vert = vertica_conn.cursor()

    cursor_vert.execute( ''' INSERT INTO STV2023070314__DWH.global_metrics (
                                date_update ,
                                currency_from ,
                                amount_total ,
                                cnt_transactions ,
                                avg_transactions_per_account ,
                                cnt_accounts_make_transactions
                                )
                            WITH total AS ( -- amount_total — общая сумма транзакций по валюте в долларах
                                SELECT 
                                    date(tr.transaction_dt) as date_update,
                                    tr.currency_code as currency_from,
                                    SUM( CASE 
                                        WHEN cr.currency_code IS NULL 
                                        THEN ABS( tr.amount ) 
                                        ELSE ABS( tr.amount / cr.currency_with_div ) 
                                    END) as amount_total
                                FROM STV2023070314__STAGING.transactions as tr
                                LEFT JOIN STV2023070314__STAGING.currencies AS cr
                                    ON date(tr.transaction_dt) = date(cr.date_update)
                                    AND tr.currency_code = cr.currency_code_with
                                    AND cr.currency_code = 420
                                WHERE tr.status = 'done'
                                GROUP BY date(tr.transaction_dt), tr.currency_code
                            ),
                            cnt_trans as (  -- cnt_transactions — общий объём транзакций по валюте
                                SELECT 
                                    date_update,
                                    currency_code as currency_from, 
                                    count(operation_id) as cnt_transactions
                                FROM (
                                    SELECT distinct	date(transaction_dt) as date_update, currency_code, operation_id
                                    from STV2023070314__STAGING.transactions
                                    where status = 'done'
                                    ) as t
                                GROUP BY date_update,currency_code
                            ),
                            avg_trans AS ( -- avg_transactions_per_account — средний объём транзакций с аккаунта
                                SELECT 
                                    date_update,
                                    currency_code as currency_from,
                                    AVG(cnt_trans_per_accounts) as avg_transactions_per_account
                                FROM (
                                    SELECT 	
                                        date(transaction_dt) as date_update,
                                        currency_code, 
                                        account_number_from, 
                                        COUNT( DISTINCT operation_id) AS cnt_trans_per_accounts
                                    from STV2023070314__STAGING.transactions
                                    where status = 'done' 
                                    GROUP BY date(transaction_dt), currency_code, account_number_from)as o
                                GROUP BY date_update, currency_code
                            ),
                            cnt_acc AS ( -- cnt_accounts_make_transactions — количество уникальных аккаунтов с совершёнными транзакциями по валюте
                                SELECT 
                                    date_update,
                                    currency_code as currency_from,
                                    count(account_number_from) AS cnt_accounts_make_transactions
                                FROM (
                                    SELECT 	date(transaction_dt) as date_update, currency_code, account_number_from 
                                    from STV2023070314__STAGING.transactions
                                    where status = 'done'
                                    union 
                                    SELECT 	date(transaction_dt) as date_update, currency_code, account_number_to 
                                    from STV2023070314__STAGING.transactions
                                    where status = 'done'
                                    ) as un
                                GROUP BY date_update, currency_code
                            )
                            SELECT 
                                t.date_update ,
                                t.currency_from ,
                                amount_total ,
                                cnt_transactions ,
                                avg_transactions_per_account ,
                                cnt_accounts_make_transactions
                            FROM total as t
                            INNER JOIN cnt_trans USING (date_update ,currency_from)
                            INNER JOIN avg_trans USING (date_update ,currency_from)
                            INNER JOIN cnt_acc USING (date_update ,currency_from)
                            WHERE t.date_update > ( SELECT date(MAX(date_update))
						                            FROM STV2023070314__DWH.global_metrics ) 
                        ''' ) 
                        

# with DAG(
#         'update_datamart',
#         default_args={'retries': 1},
#         description='Provide default dag for sprint3',
#         catchup=False,
#         start_date=datetime(2022, 10, 1),
#         schedule_interval='0 5 * * *',
#         max_active_runs = 1
# ) as dag:
    
#         update_global_datamart = PythonOperator(
#             task_id='update_global_datamart',
#             python_callable=datamart_update)


# update_global_datamart
