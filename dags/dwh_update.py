from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
   'owner': 'airflow',
   'start_date': datetime(2020, 12, 23),
   'end_date': datetime(2023, 12, 23),
   'retries': 1,
   'retry_delay': timedelta(minutes=1),
   'email': 'andrei@chenchik.me',
   'email_on_failure': True,
   'email_on_retry': True
}

dag_params = {
    'dag_id': 'dwh_update',
    'schedule_interval': "0 12 * * *",
    'catchup': False,
    'default_args': default_args
}

src = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
dest = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

def migrate_data(
    columns_to_copy, 
    source_connection, source_table, 
    destination_connection, destination_table
):
    src_conn = source_connection.get_conn()
    src_cursor = src_conn.cursor()

    load_sql = f"select {', '.join(columns_to_copy)} from {source_table}"
    src_cursor.execute(load_sql)

    destination_connection.insert_rows(
        table=destination_table, rows=src_cursor, replace=True,
        replace_index="id", target_fields=columns_to_copy
    )

with DAG(**dag_params) as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='stg_layer') as stg_layer:
        PythonOperator(
            task_id='bonussystem_ranks',
            python_callable=migrate_data,
            op_kwargs={
                'columns_to_copy': [
                    'id', 'name', 'bonus_percent', 'min_payment_threshold'
                ],
                'source_connection': src,
                'source_table': 'public.ranks',
                'destination_connection': dest,
                'destination_table': 'stg.bonussystem_ranks'}
        )

        PythonOperator(
            task_id='bonussystem_users',
            python_callable=migrate_data,
            op_kwargs={
                'columns_to_copy': [
                    'id', 'order_user_id'
                ],
                'source_connection': src,
                'source_table': 'public.users',
                'destination_connection': dest,
                'destination_table': 'stg.bonussystem_users'}
        )

    end = DummyOperator(task_id='end')

    start >> stg_layer >> end

# 4.5.2: Двигайтесь дальше! Ваш код: WHBkgRkvLo
# 4.5.3: Двигайтесь дальше! Ваш код: lgkXY8KtCn
