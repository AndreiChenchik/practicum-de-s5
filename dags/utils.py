import json

import psycopg2
from bson import json_util


def execute_one_batch(cursor, sqls, batch):
    for sql in sqls:
        psycopg2.extras.execute_values(cursor, sql, batch)


def execute_by_batch(iterable, cursor, sqls):
    batch_size = 200
    data_batch = []

    for data in iterable:
        data_batch.append(data)

        if len(data_batch) >= batch_size:
            execute_one_batch(cursor, sqls, data_batch)
            data_batch = []

    execute_one_batch(cursor, sqls, data_batch)


def fetch_wf_param(cursor, layer, db, param, default=None):
    cursor.execute(
        f"""
        select 
            workflow_settings 
        from {layer}.srv_wf_settings
        where workflow_key = '{layer}.{db}'
        order by id desc
        limit 1;
    """
    )

    latest_run = cursor.fetchone()

    try:
        latest_settings = json.loads(latest_run[0])
        value = latest_settings[param]
    except:
        value = default

    return value


def update_wf_settings(cursor, layer, db, param, value):
    current_value = fetch_wf_param(cursor, layer, db, param)

    if current_value is None or value > current_value:
        settings = {param: value}

        cursor.execute(
            f"""
            insert into {layer}.srv_wf_settings 
                (workflow_key, workflow_settings)
            values 
                ('{layer}.{db}', '{json.dumps(settings)}');
        """
        )

        return True


def extract_fields_from_bson(bson, fields):
    object = json_util.loads(bson)
    fields = [object[field] for field in fields]
    return fields


def create_bsod_row_from_object(object, fields):
    results = []

    id = str(object["_id"])
    results.append(id)

    fields = [object[field] for field in fields]
    results += fields

    json = json_util.dumps(object)
    results.append(json)

    return results
