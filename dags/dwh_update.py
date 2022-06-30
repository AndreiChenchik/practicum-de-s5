from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import psycopg2

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


def replicate_full_table(
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

def events_load(source_connection, destination_connection):
    # 1. Считали состояние с предыдущего запуска
    dest_conn = destination_connection.get_conn()
    dest_cursor = dest_conn.cursor()
    dest_cursor.execute("""
        select 
            elt_workflow_settings 
        from stg.srv_etl_settings
        where elt_workflow_key = 'bonussystem_events'
        order by id desc
        limit 1;
    """)

    last_run_select = dest_cursor.fetchone()

    try:
        last_run_settings = json.loads(last_run_select[0])
        last_loaded_id = last_run_settings["last_loaded_id"]
    except:
        last_loaded_id = 0

    # 2. Вычитали записи из таблицы outbox, в которых id больше, 
    # чем сохранённый, то есть вычитанный на первом шаге.
    src_conn = source_connection.get_conn()
    src_cursor = src_conn.cursor()    
    src_cursor.execute(f"""
        select * 
        from public.outbox 
        where id > {last_loaded_id};
    """)
    
    # 3. Сохранили данные в таблицу stg.bonussystem_events.
    batch_size = 200
    events_batch = []
    insert_batch_sql = """
        insert into stg.bonussystem_events values %s
    """
    insert_batch = lambda batch: psycopg2.extras.execute_values(
        dest_cursor, insert_batch_sql, batch
    )
    
    for data in src_cursor:
        events_batch.append(data)
        
        if len(events_batch) >= batch_size:
            insert_batch(events_batch)
            events_batch = []
    insert_batch(events_batch)
   
    # 4. Сохранили последний записанный id в таблицу stg.srv_etl_settings.
    dest_cursor.execute("""
        select 
            id 
        from stg.bonussystem_events
        order by id desc
        limit 1;
    """)
    new_last_loaded_id = dest_cursor.fetchone()[0]
    
    if new_last_loaded_id > last_loaded_id:
        last_run_settings = {"last_loaded_id": new_last_loaded_id}
        dest_cursor.execute(f"""
            insert 
                into stg.srv_etl_settings 
                    (elt_workflow_key, elt_workflow_settings)
                values 
                    ('bonussystem_events', '{json.dumps(last_run_settings)}');
        """)

        dest_conn.commit()


with DAG(**dag_params) as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='stg_layer') as stg_layer:
        PythonOperator(
            task_id='bonussystem_ranks',
            python_callable=replicate_full_table,
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
            python_callable=replicate_full_table,
            op_kwargs={
                'columns_to_copy': [
                    'id', 'order_user_id'
                ],
                'source_connection': src,
                'source_table': 'public.users',
                'destination_connection': dest,
                'destination_table': 'stg.bonussystem_users'}
        )

        PythonOperator(
            task_id='bonussystem_events',
            python_callable=events_load,
            op_kwargs={
                'source_connection': src,
                'destination_connection': dest}
        )

    end = DummyOperator(task_id='end')

    start >> stg_layer >> end

# 4.5.2: Двигайтесь дальше! Ваш код: WHBkgRkvLo
# 4.5.3: Двигайтесь дальше! Ваш код: lgkXY8KtCn
# 4.5.5: Двигайтесь дальше! Ваш код: mgXgcqQzFv 
