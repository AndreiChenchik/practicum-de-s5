from datetime import datetime
from bson import json_util
import json
from typing import List

from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient

from postgres_helpers import get_latest_run_setting, push_to_postgres

# Заводим класс для подключения к MongoDB
class MongoConnect:
    def __init__(
        self,
        cert_path: str,  # Путь до файла с сертификатом
        user: str,  # Имя пользователя БД
        pw: str,  # Пароль пользователя БД
        hosts: List[str],  # Список хостов для подключения
        rs: str,  # replica set.
        auth_db: str,  # БД для аутентификации
        main_db: str,  # БД с данными
    ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = hosts
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    # Формируем строку подключения к MongoDB
    def url(self) -> str:
        return "mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}".format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=",".join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db,
        )

    # Создаём клиент к БД
    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]


def load_from_mongo(
    source_connection,
    source_collection,
    destination_connection,
    destination_table,
):
    # Состояние с предыдущего запуска
    dest_cursor = destination_connection.cursor()

    last_loaded_ts = (
        get_latest_run_setting(dest_cursor, destination_table, "last_loaded_ts")
        or 0
    )

    # Создаём клиент к БД
    dbs = source_connection.client()

    # Объявляем параметры фильтрации
    filter = {"update_ts": {"$gt": datetime.fromtimestamp(last_loaded_ts)}}

    # Объявляем параметры сортировки
    sort = [("update_ts", 1)]

    # Вычитываем документы из MongoDB с применением фильтра и сортировки
    collection = dbs.get_collection(source_collection).find(
        filter=filter, sort=sort, batch_size=100
    )
    separate_object_id = lambda object: [
        str(object["_id"]),
        object["update_ts"],
        json_util.dumps(object),
    ]
    modified_collection = map(separate_object_id, collection)

    # Сохраняем данные в Postgres
    push_to_postgres(
        iterable=modified_collection,
        cursor=dest_cursor,
        table=destination_table,
        fields=["object_id", "update_ts", "object_value"],
    )

    # Сохраняем последний записанный update_ts в таблицу stg.srv_wf_settings.
    dest_cursor.execute(
        f"""
        select 
            update_ts 
        from {destination_table}
        order by update_ts desc
        limit 1;
    """
    )
    last_loaded_datetime = dest_cursor.fetchone()[0]
    new_last_loaded_ts = datetime.timestamp(last_loaded_datetime)

    if new_last_loaded_ts > last_loaded_ts:
        last_run_settings = {"last_loaded_ts": new_last_loaded_ts}
        dest_cursor.execute(
            f"""
            insert 
                into stg.srv_wf_settings 
                    (workflow_key, workflow_settings)
                values 
                    ('{destination_table}', '{json.dumps(last_run_settings)}');
        """
        )

        destination_connection.commit()
