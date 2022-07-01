import json

import psycopg2


def push_to_dds_from_stg(
    connection, source_table, destination_table, id_column, versioned_columns
):
    pass


def get_latest_run_setting(cursor, run_name, parameter_name):
    cursor.execute(
        f"""
        select 
            workflow_settings 
        from stg.srv_wf_settings
        where workflow_key = '{run_name}'
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


def push_to_postgres(iterable, cursor, table, fields=None):
    batch_size = 200
    events_batch = []
    fields = f"({', '.join(fields)})" if fields else ""

    insert_batch_sql = f"""
        insert into {table} {fields} values %s
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

    # 4. Сохранили последний записанный id в таблицу stg.srv_wf_settings.
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
                into stg.srv_wf_settings 
                    (workflow_key, workflow_settings)
                values 
                    ('stg.bonussystem_events', '{json.dumps(last_run_settings)}');
        """
        )

        destination_connection.commit()
