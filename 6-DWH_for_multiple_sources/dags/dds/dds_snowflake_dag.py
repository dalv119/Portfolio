import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from config_const import ConfigConst
from lib import ConnectionBuilder


from dds.dds_settings_repository import DdsEtlSettingsRepository
from dds.fct_products_loader import FctProductsLoader
from dds.order_loader import OrderLoader
from dds.products_loader import ProductLoader
from dds.restaurant_loader import RestaurantLoader
from dds.schema_ddl import SchemaDdl
from dds.timestamp_loader import TimestampLoader
from dds.user_loader import UserLoader

from dds.couriers_loader import CourierStgLoad
from dds.fct_delivery_loader import DeliveryStgLoad
from cdm.dm_courier_ledger import CourierledgerLoad
from cdm.dm_settlement_report import SettlementReportLoad

log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_case_dds_snowflake',
    # schedule_interval='0/15 * * * *',
    schedule_interval='* 0 * * *', # загрузка 1 раз в сутки
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'raw', 'dds'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    settings_repository = DdsEtlSettingsRepository()


    @task(task_id="schema_init")
    def schema_init(ds=None, **kwargs):
        rest_loader = SchemaDdl(dwh_pg_connect)
        rest_loader.init_schema()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants(ds=None, **kwargs):
        rest_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_restaurants()

    @task(task_id="dm_products_load")
    def load_dm_products(ds=None, **kwargs):
        prod_loader = ProductLoader(dwh_pg_connect, settings_repository)
        prod_loader.load_products()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()

    @task(task_id="dm_users_load")
    def load_dm_users(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users()

    @task(task_id="dm_orders_load")
    def load_dm_orders(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_orders()

    @task(task_id="fct_order_products_load")
    def load_fct_order_products(ds=None, **kwargs):
        fct_loader = FctProductsLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_product_facts()

    @task(task_id="dm_couriers")
    def load_dm_couriers(ds=None, **kwargs):
        rest_loader = CourierStgLoad(dwh_pg_connect)
        rest_loader.init_couriers()


    @task(task_id="fct_deliveries")
    def load_fct_deliveries(ds=None, **kwargs):
        rest_loader = DeliveryStgLoad(dwh_pg_connect)
        rest_loader.init_delivery()


    @task(task_id="dm_settlement_report")
    def load_dm_settlement_report(ds=None, **kwargs):
        rest_loader = SettlementReportLoad(dwh_pg_connect)
        rest_loader.init_settlement_report()

    @task(task_id="dm_courier_ledger")
    def load_dm_courier_ledger(ds=None, **kwargs):
        rest_loader = CourierledgerLoad(dwh_pg_connect)
        rest_loader.init_courier_ledger()
              
    #!  Выполнение SQL напрямую не работает. Причину так и не нашел.
    #!  Пришлось сделать через Python

    # dm_couriers_ledger_load = PostgresOperator(
    #     task_id='dm_couriers_ledger_load',
    #     postgres_conn_id="PG_WAREHOUSE_CONNECTION",
    #     sql="/lessons/dags/dds/sql/cdm_dm_couriers_ledger.sql" )  

    
    init_schema = schema_init()
    dm_restaurants = load_dm_restaurants()
    dm_products = load_dm_products()
    dm_timestamps = load_dm_timestamps()
    dm_users = load_dm_users()
    dm_orders = load_dm_orders()
    fct_order_products = load_fct_order_products()
    dm_couriers = load_dm_couriers()
    fct_deliveries = load_fct_deliveries()
    dm_courier_ledger = load_dm_courier_ledger()
    dm_settlement_report = load_dm_settlement_report()

    
    init_schema >> dm_restaurants  
    init_schema >> dm_timestamps  
    init_schema >> dm_users  
    init_schema >> dm_products  
    init_schema >> dm_orders  

    dm_restaurants >> dm_products  
    dm_restaurants >> dm_orders  
    dm_timestamps >> dm_orders  
    dm_users >> dm_orders  
    dm_products >> fct_order_products  
    dm_orders >> fct_order_products
    fct_order_products >> dm_settlement_report
     
    dm_couriers >>  dm_courier_ledger
    fct_deliveries >> dm_courier_ledger

    
    
    
    
    

   
    
