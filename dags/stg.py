from datetime import datetime
from typing import Any, Callable, List, Tuple

from mongo import MongoConnect, get_collection
from utils import (
    fetch_workflow_settings,
    update_workflow_settings,
    execute_sqls_by_batch,
    transform_data,
    drop_ms,
)

from bson import json_util

from airflow.hooks.postgres_hook import PostgresHook


def extract_bonussystem_simple(
    *,
    hook_from: PostgresHook,
    table_from: str,
    hook_to: PostgresHook,
    table_to: str,
    columns: List[str],
):
    conn = hook_from.get_conn()
    cur = conn.cursor()

    # Extract
    cur.execute(f"select {', '.join(columns)} from public.{table_from}")

    # Save
    hook_to.insert_rows(
        table=f"stg.{table_to}",
        rows=cur,
        replace=True,
        replace_index="id",
        target_fields=columns,
    )


def extract_bonussystem_events(
    *, hook_from: PostgresHook, hook_to: PostgresHook
):
    src_conn = hook_from.get_conn()
    src_cur = src_conn.cursor()

    dest_conn = hook_to.get_conn()
    dest_cur = dest_conn.cursor()

    # Previous run settings
    last_id = fetch_workflow_settings(
        cur=dest_cur,
        layer="stg",
        table="bonussystem_events",
        param="last_id",
        default=0,
    )

    # Get data
    src_cur.execute(f"select * from public.outbox where id > {last_id};")

    # Save to new place
    sql = "insert into stg.bonussystem_events values %s"
    execute_sqls_by_batch(data=src_cur, cur=dest_cur, sqls=[sql])

    # Check latest saved id
    dest_cur.execute(
        """ select id from stg.bonussystem_events
            order by id desc limit 1; """
    )
    last_id = dest_cur.fetchone()[0]

    # Save if worth it
    if update_workflow_settings(
        cur=dest_cur,
        layer="stg",
        table="bonussystem_events",
        param="last_id",
        value=last_id,
    ):
        dest_conn.commit()


def extract_ordersystem(
    *,
    mongo_from: MongoConnect,
    hook_to: PostgresHook,
    collection_from: str,
    table_to: str,
):
    client = mongo_from.client()
    conn = hook_to.get_conn()
    cur = conn.cursor()

    # Previous run settings
    last_ts = fetch_workflow_settings(
        cur=cur, layer="stg", table=table_to, param="last_ts", default=0
    )

    # Get data
    filter = {"update_ts": {"$gt": datetime.fromtimestamp(last_ts)}}
    data = get_collection(client, collection_from, filter, "update_ts")

    # Prepare for insert
    actions = [
        ("_id", str),
        ("update_ts", drop_ms),
        (".", json_util.dumps),
    ]
    data = transform_data(data=data, paths_actions=actions)  # type: ignore

    # Save to new place
    sql = f"""
        insert into stg.{table_to} 
            (object_id, update_ts, object_value) 
        values %s
    """
    execute_sqls_by_batch(data=data, cur=cur, sqls=[sql])

    # Check latest saved id
    sql = f"""
        select update_ts from stg.{table_to}
        order by update_ts desc limit 1;
    """
    cur.execute(sql)
    last_ts = datetime.timestamp(cur.fetchone()[0])

    # Save if worth it
    if update_workflow_settings(
        cur=cur, layer="stg", table=table_to, param="last_ts", value=last_ts
    ):
        conn.commit()
