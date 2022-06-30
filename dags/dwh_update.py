from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import psycopg2

from mongo import MongoConnect


def get_latest_run_setting(cursor, run_name, parameter_name):
    cursor.execute(
        f"""
        select 
            elt_workflow_settings 
        from stg.srv_etl_settings
        where elt_workflow_key = '{run_name}'
        order by id desc
        limit 1;
    """
    )

    last_run_select = cursor.fetchone()

    try:
        last_run_settings = json.loads(last_run_select[0])
        last_run_parameter = last_run_settings[parameter_name]
    except:
        last_run_parameter = None

    return last_run_parameter


def push_to_postgres(iterable, cursor, table):
    batch_size = 200
    events_batch = []
    insert_batch_sql = f"""
        insert into {table} values %s
    """
    insert_batch = lambda batch: psycopg2.extras.execute_values(
        cursor, insert_batch_sql, batch
    )

    for data in iterable:
        events_batch.append(data)

        if len(events_batch) >= batch_size:
            insert_batch(events_batch)
            events_batch = []
    insert_batch(events_batch)


def load_from_mongo(
    source_connection,
    source_collection,
    destination_connection,
    destination_table,
):
    # Состояние с предыдущего запуска
    dest_conn = destination_connection.get_conn()
    dest_cursor = dest_conn.cursor()

    last_loaded_ts = (
        get_latest_run_setting(dest_cursor, destination_table, "last_loaded_ts")
        or 0
    )

    # Создаём клиент к БД
    dbs = source_connection.client()

    # Объявляем параметры фильтрации
    filter = {"update_ts": {"$gt": datetime.fromtimestamp(last_loaded_ts)}}

    # Объявляем параметры сортировки
    sort = [("update_ts", 1)]

    # Вычитываем документы из MongoDB с применением фильтра и сортировки
    collection = dbs.get_collection(source_collection).find(
        filter=filter, sort=sort, batch_size=100
    )

    # Сохраняем данные в Postgres
    push_to_postgres(
        iterable=collection, cursor=dest_cursor, table=destination_table
    )


def replicate_postgres_table(
    columns_to_copy,
    source_connection,
    source_table,
    destination_hook,
    destination_table,
):
    src_cursor = source_connection.cursor()

    load_sql = f"select {', '.join(columns_to_copy)} from {source_table}"
    src_cursor.execute(load_sql)

    destination_hook.insert_rows(
        table=destination_table,
        rows=src_cursor,
        replace=True,
        replace_index="id",
        target_fields=columns_to_copy,
    )


def load_events_table(source_connection, destination_connection):
    # 1. Считали состояние с предыдущего запуска
    dest_cursor = destination_connection.cursor()

    last_loaded_id = (
        get_latest_run_setting(
            dest_cursor, "stg.bonussystem_events", "last_loaded_id"
        )
        or 0
    )

    # 2. Вычитали записи из таблицы outbox, в которых id больше,
    # чем сохранённый, то есть вычитанный на первом шаге.
    src_cursor = source_connection.cursor()
    src_cursor.execute(
        f"""
        select * 
        from public.outbox 
        where id > {last_loaded_id};
    """
    )

    # 3. Сохранили данные в таблицу stg.bonussystem_events.
    push_to_postgres(
        iterable=src_cursor, cursor=dest_cursor, table="stg.bonussystem_events"
    )

    # 4. Сохранили последний записанный id в таблицу stg.srv_etl_settings.
    dest_cursor.execute(
        """
        select 
            id 
        from stg.bonussystem_events
        order by id desc
        limit 1;
    """
    )
    new_last_loaded_id = dest_cursor.fetchone()[0]

    if new_last_loaded_id > last_loaded_id:
        last_run_settings = {"last_loaded_id": new_last_loaded_id}
        dest_cursor.execute(
            f"""
            insert 
                into stg.srv_etl_settings 
                    (elt_workflow_key, elt_workflow_settings)
                values 
                    ('stg.bonussystem_events', '{json.dumps(last_run_settings)}');
        """
        )

        destination_connection.commit()


dwh_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
dwh_connection = dwh_hook.get_conn()

mongo_connection = MongoConnect(
    cert_path=Variable.get("MONGO_DB_CERTIFICATE_PATH"),
    user=Variable.get("MONGO_DB_USER"),
    pw=Variable.get("MONGO_DB_PASSWORD"),
    hosts=Variable.get("MONGO_DB_HOSTS"),
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

    with TaskGroup(group_id="stg_layer") as stg_layer:
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

        # PythonOperator(
        #     task_id="ordersystem_orders",
        #     python_callable=load_from_mongo,
        #     op_kwargs={
        #         "source_connection": mongo_connection,
        #         "source_collection": "",
        #         "destination_connection": dwh_connection,
        #         "destination_table": "stg.ordersystem_orders",
        #     },
        # )

        # PythonOperator(
        #     task_id="ordersystem_restaurants",
        #     python_callable=load_from_mongo,
        #     op_kwargs={
        #         "source_connection": mongo_connection,
        #         "source_collection": "",
        #         "destination_connection": dwh_connection,
        #         "destination_table": "stg.ordersystem_restaurants",
        #     },
        # )

        # PythonOperator(
        #     task_id="ordersystem_users",
        #     python_callable=load_from_mongo,
        #     op_kwargs={
        #         "source_connection": mongo_connection,
        #         "source_collection": "",
        #         "destination_connection": dwh_connection,
        #         "destination_table": "stg.ordersystem_users",
        #     },
        # )

    end = DummyOperator(task_id="end")

    start >> stg_layer >> end

# 4.5.2: Двигайтесь дальше! Ваш код: WHBkgRkvLo
# 4.5.3: Двигайтесь дальше! Ваш код: lgkXY8KtCn
# 4.5.5: Двигайтесь дальше! Ваш код: mgXgcqQzFv
