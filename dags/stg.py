from datetime import datetime

from mongo import get_collection
from utils import (
    fetch_wf_param,
    update_wf_settings,
    execute_by_batch,
    create_bsod_row_from_object,
)


def extract_bonussystem_simple(
    destintaion_conn, source_hook, from_table, to_table, columns
):
    cursor = destintaion_conn.cursor()

    # Extract
    cursor.execute(f"select {', '.join(columns)} from public.{from_table}")

    # Save
    source_hook.insert_rows(
        table=f"stg.{to_table}",
        rows=cursor,
        replace=True,
        replace_index="id",
        target_fields=columns,
    )


def extract_bonussystem_events(source_conn, destination_conn):
    dest_cur = destination_conn.cursor()
    src_cur = source_conn.cursor()

    # Previous run settings
    last_id = fetch_wf_param(
        dest_cur, "stg", "bonussystem_events", "last_id", 0
    )

    # Get data
    src_cur.execute(f"select * from public.outbox where id > {last_id};")

    # Save to new place
    sql = "insert into stg.bonussystem_events values %s"
    execute_by_batch(data=src_cur, cursor=dest_cur, sqls=[sql])


    # Check latest saved id
    dest_cur.execute(
        """ select id from stg.bonussystem_events
            order by id desc limit 1; """
    )
    last_id = dest_cur.fetchone()[0]

    # Save if worth it
    if update_wf_settings(
        dest_cur, "stg", "bonussystem_events", "last_id", last_id
    ):
        destination_conn.commit()


def extract_ordersystem(
    source_client, destination_conn, from_collection, to_table
):
    cur = destination_conn.cursor()

    # Previous run settings
    last_ts = fetch_wf_param(cur, "stg", to_table, "last_ts", 0)

    # Get data
    filter = {"update_ts": {"$gt": datetime.fromtimestamp(last_ts)}}
    objects = get_collection(
        source_client, from_collection, filter, "update_ts"
    )
    transform = lambda object: create_bsod_row_from_object(
        object, ["update_ts"]
    )
    rows = map(transform, objects)

    # Save to new place
    sql = f"""
        insert into stg.{to_table} 
            (object_id, update_ts, object_value) 
        values %s
    """
    execute_by_batch(data=rows, cursor=cur, sqls=[sql])


    # Check latest saved id
    sql = f"""
        select update_ts from stg.{to_table}
        order by update_ts desc limit 1;
    """
    cur.execute(sql)
    last_ts = datetime.timestamp(cur.fetchone()[0])

    # Save if worth it
    if update_wf_settings(cur, "stg", to_table, "last_ts", last_ts):
        destination_conn.commit()
