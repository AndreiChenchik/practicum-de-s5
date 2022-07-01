from utils import execute_by_batch, transform_bson_row


def push_bson_object_to_scd2(
    conn, src_table, src_fields, dest_table, dest_id, dest_columns
):
    # Get the data
    cursor = conn.cursor()
    sql = f"""
        select object_id, update_ts, object_value from {src_table}
    """
    cursor.execute(sql)

    # Prepare the data
    unpack_object = lambda item: transform_bson_row(item, src_fields)
    unpacked_data = map(unpack_object, cursor)

    # Load the data with SCD2 via multiple SQL requests
    sqls = []
    # # Create brand new items
    new_items_sql = f"""
        -- add fresh ids
        with
        incoming_data ({dest_id}, update_ts, {",".join(dest_columns)}) as (
            select * from (
            values %s
            ) as external_values
        )
        insert 
            into {dest_table} (
                {dest_id},
                {",".join(dest_columns)},
                active_from,
                active_to
            ) 
            select 
                d.{dest_id},
                d.{", d.".join(dest_columns)},
                d.update_ts,
                '2099-12-31'
            from
                incoming_data d
            left join {dest_table} dt
                on dt.{dest_id} = d.{dest_id}
            where dt.id is null;
    """
    sqls.append(new_items_sql)

    # # Create updated items
    fields_comparisons = " or ".join(
        [f"dt.{field} != d.{field}" for field in dest_columns]
    )
    updated_items_sql = f"""
        -- add new version of existing ids
        with
        incoming_data ({dest_id}, update_ts, {",".join(dest_columns)}) as (
            select * from (
            values %s
            ) as external_values
        )
        insert
            into {dest_table} (
                {dest_id},
                {",".join(dest_columns)},
                active_from,
                active_to
            ) 
            select 
                d.{dest_id},
                d.{", d.".join(dest_columns)},
                d.update_ts,
                '2099-12-31'
            from
                incoming_data d
            left join {dest_table} dt
                on dt.{dest_id} = d.{dest_id} and ({fields_comparisons})
            where dt.id is not null;
    """
    sqls.append(updated_items_sql)

    # # Deactivate old items
    retire_items_sql = f"""
        -- retire old version of existed ids
        with 

            incoming_data ({dest_id}, update_ts, {",".join(dest_columns)}) as (
                select * from (
                values %s
                ) as external_values
            ),
            
            old_records (id, active_to, {dest_id}) as (
                select
                    dt.id,
                    d.update_ts,
                    d.{dest_id}
                from 
                    incoming_data d
                left join {dest_table} dt
                on dt.{dest_id} = d.{dest_id} 
                    and ({fields_comparisons}) 
                    and dt.active_to = '2099-12-31'
            )
        
        update {dest_table} dt
            set active_to=old_records.active_to
            from old_records
            where dt.id = old_records.id;
    """
    sqls.append(retire_items_sql)

    execute_by_batch(iterable=unpacked_data, cursor=cursor, sqls=sqls)
    conn.commit()
