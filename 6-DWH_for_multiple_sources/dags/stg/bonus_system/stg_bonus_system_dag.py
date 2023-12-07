import logging

import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst
from lib import ConnectionBuilder
from stg.bonus_system.event_loader import EventLoader
from stg.bonus_system.users_loader import UserLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
)
def sprint5_case_stg_bonus_system_dag():

    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
    origin_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_ORIGIN_BONUS_SYSTEM_CONNECTION)

    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect)
        user_loader.load_users()

    events = load_events()
    users = load_users()

    events  # type: ignore
    users  # type: ignore


stg_bonus_system_dag = sprint5_case_stg_bonus_system_dag()
