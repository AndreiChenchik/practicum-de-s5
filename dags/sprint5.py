from datetime import datetime

from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from mongo import MongoConnect

import stg
import dds
import cdm


remote_pg = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
dwh = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
mongo = MongoConnect(
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
            stg.extract_bonussystem_simple(
                hook_from=remote_pg,
                table_from="ranks",
                hook_to=dwh,
                table_to="bonussystem_ranks",
                columns=[
                    "id",
                    "name",
                    "bonus_percent",
                    "min_payment_threshold",
                ],
            )

        @task
        def bonussystem_users():
            stg.extract_bonussystem_simple(
                hook_from=remote_pg,
                table_from="users",
                hook_to=dwh,
                table_to="bonussystem_users",
                columns=["id", "order_user_id"],
            )

        @task
        def ordersystem_restaurants():
            stg.extract_ordersystem(
                mongo_from=mongo,
                hook_to=dwh,
                collection_from="restaurants",
                table_to="ordersystem_restaurants",
            )

        @task
        def ordersystem_users():
            stg.extract_ordersystem(
                mongo_from=mongo,
                hook_to=dwh,
                collection_from="users",
                table_to="ordersystem_users",
            )

        @task
        def ordersystem_orders():
            stg.extract_ordersystem(
                mongo_from=mongo,
                hook_to=dwh,
                collection_from="orders",
                table_to="ordersystem_orders",
            )

        def bonussystem_events():
            stg.extract_bonussystem_events(hook_from=remote_pg, hook_to=dwh)

        bonussystem_events()
        bonussystem_ranks()
        bonussystem_users()
        ordersystem_orders()
        ordersystem_restaurants()
        ordersystem_users()

    with TaskGroup(group_id="detail_store") as detail_store:

        @task
        def dm_restaurants():
            dds.transform_dm_restaurants(db_hook=dwh)

        @task
        def dm_timestamps():
            dds.transform_dm_timestamps(db_hook=dwh)

        @task
        def dm_products():
            dds.transform_dm_products(db_hook=dwh)

        @task
        def dm_orders():
            dds.transform_dm_orders(db_hook=dwh)

        @task
        def fct_product_sales():
            dds.transform_fct_product_sales(db_hook=dwh)

        dm_restaurants = dm_restaurants()
        dm_timestamps = dm_timestamps()
        dm_products = dm_products()
        dm_orders = dm_orders()
        fct_product_sales = fct_product_sales()

        dm_restaurants >> dm_products
        [dm_timestamps, dm_restaurants] >> dm_orders
        [dm_products, dm_orders] >> fct_product_sales

    with TaskGroup(group_id="data_marts") as data_marts:

        @task
        def dm_settlement_report():
            cdm.load_dm_settlement_report(conn_hook=dwh)

        dm_settlement_report()

    end = DummyOperator(task_id="end")

    start >> staging >> detail_store >> data_marts >> end


dag = sprint5()

# 4.5.2: Двигайтесь дальше! Ваш код: WHBkgRkvLo
# 4.5.3: Двигайтесь дальше! Ваш код: lgkXY8KtCn
# 4.5.5: Двигайтесь дальше! Ваш код: mgXgcqQzFv
# 4.6.2: Двигайтесь дальше! Ваш код: k2Hetyy0nu
# 4.7.2: Двигайтесь дальше! Ваш код: mgXgcqQzFv
# 4.7.3: Двигайтесь дальше! Ваш код: WXcbW1NLh9
# 4.7.4: Двигайтесь дальше! Ваш код: y7M8bxX1z9
# 4.7.5: Двигайтесь дальше! Ваш код: 8i8NjzMWsa
# 4.7.6: Двигайтесь дальше! Ваш код: jemju9gmX7
# 4.8.1: Двигайтесь дальше! Ваш код: dUY4sNFuOZ
