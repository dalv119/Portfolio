# Шаг 5. Автоматизировать обновление витрин

# С помощью Airflow автоматизируйте все три скрипта, которые написали для построения витрин. 
# Правильно настройте время обновления витрин. DAG сохраните в папку dags, а скрипты — в папку scripts.

from datetime import datetime 
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2020, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "sparkoperator_project",
                        default_args=default_args,
                        schedule_interval='0 0 * * 7',
                        )

# объявляем задачу с помощью SparkSubmitOperator

task_1 = SparkSubmitOperator(
                        task_id='user_mart',
                        dag=dag_spark,
                        application ='/user/vladimirdo/user_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["2022-05-31", 30, '/user/master/data/geo/events/', '/user/vladimirdo/geo.csv', '/user/vladimirdo/data/analitics/'],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

task_2 = SparkSubmitOperator(
                        task_id='city_mart',
                        dag=dag_spark,
                        application ='/user/vladimirdo/city_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["2022-05-31", 30, '/user/master/data/geo/events/', '/user/vladimirdo/geo.csv', '/user/vladimirdo/data/analitics/'],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

task_3 = SparkSubmitOperator(
                        task_id='friends_mart',
                        dag=dag_spark,
                        application ='/user/vladimirdo/city_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["2022-05-31", 30, '/user/master/data/geo/events/', '/user/vladimirdo/geo.csv', '/user/vladimirdo/data/analitics/'],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


[task_1, task_2, task_3]