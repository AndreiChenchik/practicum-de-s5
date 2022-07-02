from datetime import datetime
from bson import json_util
from typing import List

from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient

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


def get_collection(client, collection, filter, sorted_by=None):
    sort = [(sorted_by, 1)] if sorted_by else None

    collection = client.get_collection(collection).find(
        filter=filter, sort=sort, batch_size=100
    )
    return collection
