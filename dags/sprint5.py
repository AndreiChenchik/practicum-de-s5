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
    schedule_interval="@daily",
    start_date=datetime(2022, 7, 1),
    catchup=False,
)
def sprint5():
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id="staging") as staging:

        @task
        def bonussystem_ranks():
            columns = ["id", "name", "bonus_percent", "min_payment_threshold"]
            stg.extract_bonussystem_simple(
                source_hook=dwh_hook,
                destintaion_conn=db_conn,
                from_table="ranks",
                to_table="bonussystem_ranks",
                columns=columns,
            )

        @task
        def bonussystem_users():
            columns = ["id", "order_user_id"]
            stg.extract_bonussystem_simple(
                source_hook=dwh_hook,
                destintaion_conn=db_conn,
                from_table="users",
                to_table="bonussystem_users",
                columns=columns,
            )

        @task
        def ordersystem_restaurants():
            client = mongo_connection.client()
            stg.extract_ordersystem(
                source_client=client,
                destination_conn=dwh_conn,
                from_collection="restaurants",
                to_table="ordersystem_restaurants",
            )

        @task
        def ordersystem_users():
            client = mongo_connection.client()
            stg.extract_ordersystem(
                source_client=client,
                destination_conn=dwh_conn,
                from_collection="users",
                to_table="ordersystem_users",
            )

        @task
        def ordersystem_orders():
            client = mongo_connection.client()
            stg.extract_ordersystem(
                source_client=client,
                destination_conn=dwh_conn,
                from_collection="orders",
                to_table="ordersystem_orders",
            )

        @task
        def bonussystem_events():
            stg.extract_bonussystem_events(
                source_conn=db_conn, destination_conn=dwh_conn
            )

        # bonussystem_events()
        # bonussystem_ranks()
        # bonussystem_users()
        ordersystem_orders()
        ordersystem_restaurants()
        ordersystem_users()

    with TaskGroup(group_id="detail_store") as detail_store:

        @task
        def dm_restaurants():
            dds.transform_dm_restaurants(conn=dwh_conn)

        @task
        def dm_timestamps():
            dds.transform_dm_timestamps(conn=dwh_conn)

        @task
        def dm_products():
            dds.transform_dm_products(conn=dwh_conn)

        @task
        def dm_orders():
            dds.transform_dm_orders(conn=dwh_conn)

        dm_restaurants = dm_restaurants()
        dm_timestamps = dm_timestamps()
        dm_products = dm_products()
        dm_orders = dm_orders()

        dm_timestamps >> dm_orders
        dm_restaurants >> dm_orders
        dm_restaurants >> dm_products

    end = DummyOperator(task_id="end")

    start >> staging >> detail_store >> end


dag = sprint5()

# 4.5.2: Двигайтесь дальше! Ваш код: WHBkgRkvLo
# 4.5.3: Двигайтесь дальше! Ваш код: lgkXY8KtCn
# 4.5.5: Двигайтесь дальше! Ваш код: mgXgcqQzFv
# 4.6.2: Двигайтесь дальше! Ваш код: k2Hetyy0nu
# 4.7.2: Двигайтесь дальше! Ваш код: mgXgcqQzFv
# 4.7.3: Двигайтесь дальше! Ваш код: WXcbW1NLh9
# 4.7.4: Двигайтесь дальше! Ваш код: y7M8bxX1z9
# 4.7.5: Двигайтесь дальше! Ваш код: 8i8NjzMWsa
