#!/usr/bin/env sh

docker exec s5-lessons-de-pg-cr-af-1 airflow connections add 'PG_WAREHOUSE_CONNECTION2' \
    --conn-json '{
        "conn_type": "postgres",
        "login": "jovyan",
        "password": "jovyan",
        "host": "localhost",
        "port": 5432,
        "schema": "de"
    }'


docker exec s5-lessons-de-pg-cr-af-1 airflow connections add 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION2' \
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