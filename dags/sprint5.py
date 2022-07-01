from datetime import datetime

from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from mongo import MongoConnect
import stg
import dds

db_hook = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
db_conn = db_hook.get_conn()

dwh_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
dwh_conn = dwh_hook.get_conn()

mongo_connection = MongoConnect(
    cert_path=Variable.get("MONGO_DB_CERTIFICATE_PATH"),
    user=Variable.get("MONGO_DB_USER"),
    pw=Variable.get("MONGO_DB_PASSWORD"),
    hosts=[Variable.get("MONGO_DB_HOSTS")],
    rs=Variable.get("MONGO_DB_REPLICA_SET"),
    auth_db=Variable.get("MONGO_DB_DATABASE_NAME"),
    main_db=Variable.get("MONGO_DB_DATABASE_NAME"),
)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=datetime(2020, 12, 23),
    catchup=False,
)
def sprint5():
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id="staging") as staging:

        @task
        def bonussystem_ranks():
            columns = ["id", "name", "bonus_percent", "min_payment_threshold"]
            stg.bonussystem_simple(
                source_hook=dwh_hook,
                destintaion_conn=db_conn,
                from_table="ranks",
                to_table="bonussystem_ranks",
                columns=columns,
            )

        @task
        def bonussystem_users():
            columns = ["id", "order_user_id"]
            stg.bonussystem_simple(
                source_hook=dwh_hook,
                destintaion_conn=db_conn,
                from_table="users",
                to_table="bonussystem_users",
                columns=columns,
            )

        @task
        def ordersystem_restaurants():
            client = mongo_connection.client()
            stg.ordersystem(
                source_client=client,
                destination_conn=dwh_conn,
                from_collection="restaurants",
                to_table="ordersystem_restaurants",
            )

        @task
        def ordersystem_users():
            client = mongo_connection.client()
            stg.ordersystem(
                source_client=client,
                destination_conn=dwh_conn,
                from_collection="users",
                to_table="ordersystem_users",
            )

        @task
        def ordersystem_orders():
            client = mongo_connection.client()
            stg.ordersystem(
                source_client=client,
                destination_conn=dwh_conn,
                from_collection="orders",
                to_table="ordersystem_orders",
            )

        @task
        def bonussystem_events():
            stg.bonussystem_events(
                source_conn=db_conn, destination_conn=dwh_conn
            )

        bonussystem_events()
        bonussystem_ranks()
        bonussystem_users()
        ordersystem_orders()
        ordersystem_restaurants()
        ordersystem_users()

    with TaskGroup(group_id="detail_store") as detail_store:

        @task
        def dm_restaurants():
            dds.push_bson_object_to_scd2(
                conn=dwh_conn,
                src_table="stg.ordersystem_restaurants",
                src_fields=["name"],
                dest_columns=["restaurant_name"],
                dest_id="restaurant_id",
                dest_table="dds.dm_restaurants",
            )

        dm_restaurants()

    end = DummyOperator(task_id="end")

    start >> staging >> detail_store >> end


dag = sprint5()

# 4.5.2: Двигайтесь дальше! Ваш код: WHBkgRkvLo
# 4.5.3: Двигайтесь дальше! Ваш код: lgkXY8KtCn
# 4.5.5: Двигайтесь дальше! Ваш код: mgXgcqQzFv
# 4.6.2: Двигайтесь дальше! Ваш код: k2Hetyy0nu
# 4.7.2: Двигайтесь дальше! Ваш код: mgXgcqQzFv
