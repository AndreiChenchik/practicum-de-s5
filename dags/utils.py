import json
from typing import Dict, Iterable, List

import psycopg2.extras
from bson import json_util


def execute_by_batch(*, cur, sqls: List[str], data: Iterable):
    max_batch_size = 200
    batch: list = []

    execute_values = lambda sql: psycopg2.extras.execute_values(cur, sql, batch)

    for item in data:
        batch.append(item)

        if len(batch) >= max_batch_size:
            _ = list(map(execute_values, sqls))
            batch = []

    _ = list(map(execute_values, sqls))


def fetch_workflow_settings(
    *, cur, layer: str, table: str, param: str, default=None
):
    cur.execute(
        f"""
        select 
            workflow_settings 
        from {layer}.srv_wf_settings
        where workflow_key = '{table}'
        order by id desc
        limit 1;
    """
    )

    latest_run = cur.fetchone()

    try:
        latest_settings = json.loads(latest_run[0])
        value = latest_settings[param]
    except:
        value = default

    return value


def update_workflow_settings(*, cur, layer: str, table: str, param: str, value):
    current_value = fetch_workflow_settings(
        cur=cur, layer=layer, table=table, param=param
    )

    if current_value is None or value > current_value:
        settings = {param: value}

        cur.execute(
            f"""
            insert into {layer}.srv_wf_settings 
                (workflow_key, workflow_settings)
            values 
                ('{table}', '{json.dumps(settings)}');
        """
        )

        return True


def extract_fields_from_bson(*, bson: str, fields: List[str]):
    object = json_util.loads(bson)
    fields = [object[field] for field in fields]
    return fields


def create_bsod_row_from_object(*, object: Dict, fields: List[str]):
    results = []

    id = str(object["_id"])
    results.append(id)

    fields = [object[field] for field in fields]
    results += fields

    json = json_util.dumps(object)
    results.append(json)

    return results
