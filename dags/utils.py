import json
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable, Union

import psycopg2.extras

from datetime import datetime


def execute_sqls_by_batch(*, cur, sqls: List[str], data: Iterable):
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


drop_ms: Callable[[datetime], datetime] = lambda dt: dt.replace(microsecond=0)


def extract_field(*, object, path: str):
    if path == ".":
        return object

    for index in path.split("."):
        try:
            object = object[int(index)]
        except:
            object = object[index]

    return object


def apply_action(
    *,
    object: Union[Dict, List],
    path_action: Tuple[str, Callable[[Any], Any]],
):
    path, action = path_action
    value = extract_field(object=object, path=path)

    if action:
        return action(value)
    else:
        return value


def transform_data(
    *,
    data: Iterable,
    paths_actions: List,
    list_path: Optional[str] = None,
    list_paths_actions: Optional[List] = None,
):
    for object in data:

        result: List[Any] = []

        apply = lambda pa: apply_action(path_action=pa, object=object)
        result += map(apply, paths_actions)

        if list_path is None or list_paths_actions is None:
            yield result
        else:
            list_to_expand = extract_field(object=object, path=list_path)

            for object in list_to_expand:
                additional_fields: List[Any] = []

                apply = lambda pa: apply_action(path_action=pa, object=object)
                additional_fields += map(apply, list_paths_actions)

                yield result + additional_fields
