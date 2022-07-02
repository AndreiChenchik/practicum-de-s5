from utils import execute_by_batch, extract_fields_from_bson

future_date = "2099-12-31"


def get_data_from_bsod_table(cursor, source_table, object_fields):
    # Get the data
    sql = f"""
        select object_id, update_ts, object_value from {source_table}
    """
    cursor.execute(sql)

    # Prepare the data
    unpack_object = lambda item: [
        item[0],
        item[1].replace(microsecond=0),
    ] + extract_fields_from_bson(item[2], object_fields)

    unpacked_data = map(unpack_object, cursor)

    return unpacked_data


def prepare_sdc2_sql(data_cte_sql, table, id, columns):
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


def extract_date_details(data):
    for item in data:
        if item[3] not in ["CANCELLED", "CLOSED"]:
            continue

        ts = item[2].replace(microsecond=0)
        date = item[2].replace(microsecond=0).date()
        time = item[2].replace(microsecond=0).time()
        year = item[2].replace(microsecond=0).year
        month = item[2].replace(microsecond=0).month
        day = item[2].replace(microsecond=0).day

        yield [ts, date, time, year, month, day]


def transform_dm_timestamps(conn_hook):
    conn = conn_hook.get_conn()
    src_cur = conn.cursor()

    source_table = "stg.ordersystem_orders"
    object_fields = ["date", "final_status"]
    data = get_data_from_bsod_table(src_cur, source_table, object_fields)

    data = extract_date_details(data)

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
    execute_by_batch(data=data, cursor=dest_cur, sqls=[sql])
    conn.commit()


def transform_dm_restaurants(conn_hook):
    conn = conn_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_restaurants"
    object_fields = ["name"]
    data = get_data_from_bsod_table(src_cursor, source_table, object_fields)

    table = "dds.dm_restaurants"
    id = "restaurant_id"
    columns = ["restaurant_name"]
    data_cte_sql = f"""
        with
            data ({id}, update_ts, {", ".join(columns)}) as (
                select * from (values %s) as external_values
            )
    """

    sqls = prepare_sdc2_sql(data_cte_sql, table, id, columns)

    dest_cursor = conn.cursor()
    execute_by_batch(data=data, cursor=dest_cursor, sqls=sqls)
    conn.commit()


def extract_menu(data):
    for item in data:
        restaurant_id = item[0]
        update_ts = item[1]
        menu = item[2]

        menu.sort(key=lambda product: str(product["_id"]))
        for product in menu:
            id = str(product["_id"])
            name = product["name"]
            price = product["price"]

            yield [id, update_ts, restaurant_id, name, price]


def transform_dm_products(conn_hook):
    conn = conn_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_restaurants"
    object_fields = ["menu"]
    data = get_data_from_bsod_table(src_cursor, source_table, object_fields)

    data = extract_menu(data)

    table = "dds.dm_products"
    id = "product_id"
    columns = ["restaurant_id", "product_name", "product_price"]
    data_cte_sql = f"""
        with
            pre_data ({id}, update_ts, {", ".join(columns)}) as (
                select * from (values %s) as external_values
            ),

            data ({id}, update_ts, {", ".join(columns)}) as (
                select 
                    pd.{id},
                    pd.update_ts, 
                    dmr.id,
                    pd.{", pd.".join(columns[1:])}
                from pre_data pd
                left join dds.dm_restaurants dmr
                    on dmr.restaurant_id = pd.restaurant_id 
                        and dmr.active_to = '{future_date}'
            )
    """

    sqls = prepare_sdc2_sql(data_cte_sql, table, id, columns)

    dest_cursor = conn.cursor()
    execute_by_batch(data=data, cursor=dest_cursor, sqls=sqls)
    conn.commit()


def transform_dm_orders(conn_hook):
    conn = conn_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_orders"
    object_fields = ["date", "final_status", "user", "restaurant"]
    data = get_data_from_bsod_table(src_cursor, source_table, object_fields)

    extract_order_info = lambda item: [
        item[0],  # order_key
        item[1],  # update_ts
        item[2].replace(microsecond=0),  # timestamp
        item[3],  # order_status
        str(item[4]["id"]),  # user_key
        str(item[5]["id"]),  # restaurant_key
    ]
    data = map(extract_order_info, data)

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
    execute_by_batch(data=data, cursor=dest_cursor, sqls=[sql])
    conn.commit()


def extract_order_items(data):
    for item in data:
        order_key = item[0]
        update_ts = item[1]
        total_sum = item[3]
        bonus_payment = item[4]
        bonus_grant = item[5]
        cart = item[2]

        for product in cart:
            product_key = str(product["id"])
            price = product["price"]
            count = product["quantity"]

            yield [
                order_key,
                update_ts,
                total_sum,
                bonus_payment,
                bonus_grant,
                product_key,
                price,
                count,
            ]


def transform_fct_product_sales(conn_hook):
    conn = conn_hook.get_conn()
    src_cursor = conn.cursor()

    source_table = "stg.ordersystem_orders"
    object_fields = [
        "order_items",
        "cost",
        "payment",
        "bonus_payment",
        "bonus_grant",
    ]
    data = get_data_from_bsod_table(src_cursor, source_table, object_fields)

    data = extract_order_items(data)

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
    execute_by_batch(data=data, cursor=dest_cursor, sqls=[sql])
    conn.commit()
