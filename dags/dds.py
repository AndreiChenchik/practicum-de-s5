from utils import execute_by_batch, transform_bson_row


def transform_dm_with_scd2(
    conn,
    source_table,
    object_fields,
    destination_table,
    destination_id,
    destination_columns,
):
    # Get the data
    cursor = conn.cursor()
    sql = f"""
        select object_id, update_ts, object_value from {source_table}
    """
    cursor.execute(sql)

    # Prepare the data
    unpack_object = lambda item: transform_bson_row(item, object_fields)
    unpacked_data = map(unpack_object, cursor)

    # Load the data with SCD2 via multiple SQL requests
    sqls = []
    # # Create brand new items
    new_items_sql = f"""
        -- add fresh ids
        with
            incoming_data (
                {destination_id},
                update_ts,
                {",".join(destination_columns)}
            ) as (
                select * from (
                values %s
                ) as external_values
            )
        insert 
            into {destination_table} (
                {destination_id},
                {",".join(destination_columns)},
                active_from,
                active_to
            ) 
            select 
                d.{destination_id},
                d.{", d.".join(destination_columns)},
                d.update_ts,
                '2099-12-31'
            from
                incoming_data d
            left join {destination_table} dt
                on dt.{destination_id} = d.{destination_id}
            where dt.id is null;
    """
    sqls.append(new_items_sql)

    # # Create updated items
    fields_comparisons = " or ".join(
        [f"dt.{field} != d.{field}" for field in destination_columns]
    )
    updated_items_sql = f"""
        -- add new version of existing ids
        with
            incoming_data (
                {destination_id},
                update_ts,
                {",".join(destination_columns)}
            ) as (
                select * from (
                values %s
                ) as external_values
            )
        insert
            into {destination_table} (
                {destination_id},
                {",".join(destination_columns)},
                active_from,
                active_to
            ) 
            select 
                d.{destination_id},
                d.{", d.".join(destination_columns)},
                d.update_ts,
                '2099-12-31'
            from
                incoming_data d
            left join {destination_table} dt
                on dt.{destination_id} = d.{destination_id} and ({fields_comparisons})
            where dt.id is not null;
    """
    sqls.append(updated_items_sql)

    # # Deactivate old items
    retire_items_sql = f"""
        -- retire old version of existed ids
        with 

            incoming_data (
                {destination_id},
                update_ts,
                {",".join(destination_columns)}
            ) as (
                select * from (
                values %s
                ) as external_values
            ),
            
            old_records (id, active_to, {destination_id}) as (
                select
                    dt.id,
                    d.update_ts,
                    d.{destination_id}
                from 
                    incoming_data d
                left join {destination_table} dt
                on dt.{destination_id} = d.{destination_id} 
                    and ({fields_comparisons}) 
                    and dt.active_to = '2099-12-31'
            )
        
        update {destination_table} dt
            set active_to=old_records.active_to
            from old_records
            where dt.id = old_records.id;
    """
    sqls.append(retire_items_sql)

    execute_by_batch(iterable=unpacked_data, cursor=cursor, sqls=sqls)
    conn.commit()
