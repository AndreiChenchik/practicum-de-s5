#!/usr/bin/env sh

airflow connections add 'PG_WAREHOUSE_CONNECTION' \
    --conn-json '{
        "conn_type": "postgres",
        "login": "jovyan",
        "password": "jovyan",
        "host": "localhost",
        "port": 5432,
        "schema": "de"
    }'


airflow connections add 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION' \
    --conn-json '{
        "conn_type": "postgres",
        "login": "student",
        "password": "student1",
        "host": "rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net",
        "port": 6432,
        "schema": "de-public",
        "extra": {
            "sslmode": "require"
        }
    }'

# Двигайтесь дальше! Ваш код: gESZ89Tpop