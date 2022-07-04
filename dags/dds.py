from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from bson import json_util

from utils import execute_sqls_by_batch, transform_data, drop_ms

future_date = "2099-12-31"
bsod_table_select_sql = (
    lambda *, for_table: f"""
        select object_id, update_ts, object_value from {for_table}
    """
)


def prepare_sdc2_sql(
    *, data_cte_sql: str, table: str, id: str, columns: List[str]
):
    # Transform the data with SCD2 via multiple SQL requests
    sqls = []
    # # Create brand new items
    new_items_sql = f"""
        -- add fresh ids
        {data_cte_sql}
        
        insert 
            into {table} ({id}, {",".join(columns)}, active_from, active_to) 
            select 
                d.{id}, d.{", d.".join(columns)}, d.update_ts, '{future_date}'
            from
                data d
            left join {table} dt
                on dt.{id} = d.{id}
            where dt.id is null
            order by {id};
    """
    sqls.append(new_items_sql)

    # # Create updated items
    fields_comparisons = " or ".join(
        [f"dt.{field} != d.{field}" for field in columns]
    )
    updated_items_sql = f"""
        -- add new version of existing ids
        {data_cte_sql}
        
        insert
            into {table} ({id}, {",".join(columns)}, active_from, active_to) 
            select 
                d.{id}, d.{", d.".join(columns)}, d.update_ts, '{future_date}'
            from
                data d
            left join {table} dt
                on dt.{id} = d.{id} and ({fields_comparisons})
            where dt.id is not null
            order by {id};
    """
    sqls.append(updated_items_sql)

    # # Deactivate old items
    old_records_cte_sql = f"""
        {data_cte_sql},
            
            old_records (id, active_to, {id}) as (
                select
                    dt.id, d.update_ts, d.{id}
                from 
                    data d
                left join {table} dt
                    on dt.{id} = d.{id} 
                        and ({fields_comparisons}) 
                        and dt.active_to = '{future_date}'
                order by {id}
            )
    """
    retire_items_sql = f"""
        -- retire old version of existed ids
        {old_records_cte_sql}
        
        update {table} dt
            set active_to=old_records.active_to
            from old_records
            where dt.id = old_records.id;
    """
    sqls.append(retire_items_sql)

    return sqls


def transform_dm_timestamps(*, db_hook: PostgresHook):
    conn = db_hook.get_conn()
    src_cur = conn.cursor()

    source_table = "stg.ordersystem_orders"
    src_cur.execute(bsod_table_select_sql(for_table=source_table))
    data = src_cur

    fields: List = [("2", json_util.loads)]  # full order
    data = transform_data(data=data, paths_actions=fields)

    fields = [
        ("0.date", drop_ms),  # order date
        ("0.final_status", None),  # order status
    ]
    data = transform_data(data=data, paths_actions=fields)

    data = filter(lambda item: item[1] in ["CANCELLED", "CLOSED"], data)

    fields = [
        ("0", None),  # ts
        ("0", lambda ts: ts.date()),  # date
        ("0", lambda ts: ts.time()),  # time
        ("0", lambda ts: ts.year),  # year
        ("0", lambda ts: ts.month),  # month
        ("0", lambda ts: ts.day),  # day
    ]
    data = transform_data(data=data, paths_actions=fields)

    sql = """
        with
            data (ts, date, time, year, month, day) as (
                select * from (values %s) as external_values
            )
        insert 
            into dds.dm_timestamps
                (ts, date, time, year, month, day)
            select distinct *
            from data
    """

    dest_cur = conn.cursor()
    execute_sqls_by_batch(data=data, cur=dest_cur, sqls=[sql])
    conn.commit()


def transform_dm_restaurants(*, db_hook: PostgresHook):
    conn = db_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_restaurants"
    src_cursor.execute(bsod_table_select_sql(for_table=source_table))
    data = src_cursor

    fields: List = [
        ("0", str),  # restaurant_id
        ("1", None),  # update_ts
        ("2", json_util.loads),  # full restaurant object
    ]
    data = transform_data(data=data, paths_actions=fields)

    fields = [
        ("0", None),  # restaurant_id
        ("1", None),  # update_ts
        ("2.name", None),  # restaurant_name
    ]
    data = transform_data(data=data, paths_actions=fields)

    table = "dds.dm_restaurants"
    sdc2_id = "restaurant_id"
    sdc2_columns = ["restaurant_name"]
    data_cte_sql = f"""
        with
            data ({sdc2_id}, update_ts, {", ".join(sdc2_columns)}) as (
                select * from (values %s) as external_values
            )
    """

    sqls = prepare_sdc2_sql(
        data_cte_sql=data_cte_sql, table=table, id=sdc2_id, columns=sdc2_columns
    )

    dest_cursor = conn.cursor()
    execute_sqls_by_batch(data=data, cur=dest_cursor, sqls=sqls)

    conn.commit()


def transform_dm_products(*, db_hook: PostgresHook):
    conn = db_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_restaurants"
    src_cursor.execute(bsod_table_select_sql(for_table=source_table))
    data = src_cursor

    fields: List = [
        ("0", str),  # restaurant_id
        ("1", None),  # update_ts
        ("2", json_util.loads),  # full restaurant object
    ]
    data = transform_data(data=data, paths_actions=fields)

    fields = [
        ("1", None),  # update_ts
        ("0", None),  # restaurant_id
    ]
    list_fields = [
        ("name", None),  # product_name
        ("price", None),  # product_price
        ("_id", str),  # product_id
    ]
    data = transform_data(
        data=data,
        paths_actions=fields,
        list_path="2.menu",
        list_paths_actions=list_fields,
    )

    sdc2_table = "dds.dm_products"
    sdc2_id = "product_id"
    sdc2_columns = ["restaurant_id", "product_name", "product_price"]
    data_cte_sql = f"""
        with
            pre_data (update_ts, {", ".join(sdc2_columns)}, {sdc2_id}) as (
                select * from (values %s) as external_values
            ),

            data ({sdc2_id}, update_ts, {", ".join(sdc2_columns)}) as (
                select 
                    pd.{sdc2_id},
                    pd.update_ts, 
                    dmr.id,
                    pd.{", pd.".join(sdc2_columns[1:])}
                from pre_data pd
                left join dds.dm_restaurants dmr
                    on dmr.restaurant_id = pd.restaurant_id 
                        and dmr.active_to = '{future_date}'
            )
    """
    sqls = prepare_sdc2_sql(
        data_cte_sql=data_cte_sql,
        table=sdc2_table,
        id=sdc2_id,
        columns=sdc2_columns,
    )

    dest_cursor = conn.cursor()
    execute_sqls_by_batch(data=data, cur=dest_cursor, sqls=sqls)
    conn.commit()


def transform_dm_orders(*, db_hook: PostgresHook):
    conn = db_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_orders"
    src_cursor.execute(bsod_table_select_sql(for_table=source_table))
    data = src_cursor

    fields: List = [
        ("0", str),  # order_id
        ("1", None),  # update_ts
        ("2", json_util.loads),  # full order object
    ]
    data = transform_data(data=data, paths_actions=fields)

    fields = [
        ("0", None),  # order_key
        ("1", None),  # update_ts
        ("2.date", drop_ms),  # timestamp
        ("2.final_status", None),  # order_status
        ("2.user.id", str),  # user_key
        ("2.restaurant.id", str),  # restaurant_key
    ]
    data = transform_data(data=data, paths_actions=fields)

    sql = f"""
        with data (
            order_key, update_ts, timestamp, order_status, ukey, rkey
        ) as (select * from (values %s) as external_values)

        insert 
            into dds.dm_orders
                (order_key, order_status, restaurant_id, user_id, timestamp_id)
            select 
                order_key, order_status, dmr.id, dmu.id, dmt.id
            from data d
            left join dds.dm_restaurants dmr
                on dmr.restaurant_id = d.rkey
                    and dmr.active_to = '{future_date}' 
            left join dds.dm_users dmu
                on dmu.user_id = d.ukey
            left join dds.dm_timestamps dmt
                on dmt.ts = d.timestamp 
    """

    dest_cursor = conn.cursor()
    execute_sqls_by_batch(data=data, cur=dest_cursor, sqls=[sql])

    conn.commit()


def transform_fct_product_sales(*, db_hook: PostgresHook):
    conn = db_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_orders"
    src_cursor.execute(bsod_table_select_sql(for_table=source_table))
    data = src_cursor

    fields: List = [
        ("0", str),  # order_id
        ("1", None),  # update_ts
        ("2", json_util.loads),  # full order object
    ]
    data = transform_data(data=data, paths_actions=fields)

    fields = [
        ("0", None),  # order_key
        ("1", None),  # update_ts
        ("2.payment", None),  # total_sum
        ("2.bonus_payment", None),  # bonus_payment
        ("2.bonus_grant", None),  # bonus_grant
    ]
    list_fields = [
        ("id", str),  # product_key
        ("price", None),  # price
        ("quantity", None),  # count
    ]
    data = transform_data(
        data=data,
        paths_actions=fields,
        list_path="2.order_items",
        list_paths_actions=list_fields,
    )

    sql = f"""
        with data (
            order_key, update_ts, total_sum, bonus_payment, bonus_grant,
            product_key, price, count
        ) as (select * from (values %s) as external_values)

        insert 
            into dds.fct_product_sales (
                order_id, total_sum, bonus_payment, bonus_grant, 
                product_id, price, count
            )
            select 
                dmo.id, total_sum, bonus_payment, bonus_grant,
                dmp.id, price, count
            from data d
            left join dds.dm_orders dmo
                on dmo.order_key = d.order_key
            left join dds.dm_products dmp
                on dmp.product_id = d.product_key
                    and dmp.active_to = '{future_date}'
    """

    dest_cursor = conn.cursor()
    execute_sqls_by_batch(data=data, cur=dest_cursor, sqls=[sql])

    conn.commit()
