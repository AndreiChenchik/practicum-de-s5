from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from mongo_helpers import MongoConnect, load_from_mongo
from postgres_helpers import (
    replicate_postgres_table,
    load_events_table,
    push_to_dds_from_stg,
)


dwh_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
dwh_connection = dwh_hook.get_conn()

mongo_connection = MongoConnect(
    cert_path=Variable.get("MONGO_DB_CERTIFICATE_PATH"),
    user=Variable.get("MONGO_DB_USER"),
    pw=Variable.get("MONGO_DB_PASSWORD"),
    hosts=[Variable.get("MONGO_DB_HOSTS")],
    rs=Variable.get("MONGO_DB_REPLICA_SET"),
    auth_db=Variable.get("MONGO_DB_DATABASE_NAME"),
    main_db=Variable.get("MONGO_DB_DATABASE_NAME"),
)

postgres_hook = PostgresHook(
    postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
)
postgres_connection = postgres_hook.get_conn()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 12, 23),
    "end_date": datetime(2023, 12, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email": "andrei@chenchik.me",
    "email_on_failure": True,
    "email_on_retry": True,
}

dag_params = {
    "dag_id": "dwh_update",
    "schedule_interval": "0 12 * * *",
    "catchup": False,
    "default_args": default_args,
}

with DAG(**dag_params) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id="staging") as staging:
        PythonOperator(
            task_id="bonussystem_ranks",
            python_callable=replicate_postgres_table,
            op_kwargs={
                "columns_to_copy": [
                    "id",
                    "name",
                    "bonus_percent",
                    "min_payment_threshold",
                ],
                "source_connection": postgres_connection,
                "source_table": "public.ranks",
                "destination_hook": dwh_hook,
                "destination_table": "stg.bonussystem_ranks",
            },
        )

        PythonOperator(
            task_id="bonussystem_users",
            python_callable=replicate_postgres_table,
            op_kwargs={
                "columns_to_copy": ["id", "order_user_id"],
                "source_connection": postgres_connection,
                "source_table": "public.users",
                "destination_hook": dwh_hook,
                "destination_table": "stg.bonussystem_users",
            },
        )

        PythonOperator(
            task_id="bonussystem_events",
            python_callable=load_events_table,
            op_kwargs={
                "source_connection": postgres_connection,
                "destination_connection": dwh_connection,
            },
        )

        PythonOperator(
            task_id="ordersystem_orders",
            python_callable=load_from_mongo,
            op_kwargs={
                "source_connection": mongo_connection,
                "source_collection": "orders",
                "destination_connection": dwh_connection,
                "destination_table": "stg.ordersystem_orders",
            },
        )

        PythonOperator(
            task_id="ordersystem_restaurants",
            python_callable=load_from_mongo,
            op_kwargs={
                "source_connection": mongo_connection,
                "source_collection": "restaurants",
                "destination_connection": dwh_connection,
                "destination_table": "stg.ordersystem_restaurants",
            },
        )

        PythonOperator(
            task_id="ordersystem_users",
            python_callable=load_from_mongo,
            op_kwargs={
                "source_connection": mongo_connection,
                "source_collection": "users",
                "destination_connection": dwh_connection,
                "destination_table": "stg.ordersystem_users",
            },
        )

    with TaskGroup(group_id="detail_store") as detail_store:
        PythonOperator(
            task_id="dm_restaurants",
            python_callable=push_to_dds_from_stg,
            op_kwargs={
                "connection": dwh_connection,
                "source_table": "stg.ordersystem_restaurants",
                "destination_table": "dds.dm_restaurants",
                "id_column": "restaurant_id",
                "versioned_columns": ["restaurant_name"],
            },
        )

    end = DummyOperator(task_id="end")

    start >> staging >> detail_store >> end

# 4.5.2: Двигайтесь дальше! Ваш код: WHBkgRkvLo
# 4.5.3: Двигайтесь дальше! Ваш код: lgkXY8KtCn
# 4.5.5: Двигайтесь дальше! Ваш код: mgXgcqQzFv
# 4.6.2: Двигайтесь дальше! Ваш код: k2Hetyy0nu
