drop schema if exists dds cascade;
create schema dds;

drop table if exists dds.dm_users;
CREATE TABLE dds.dm_users
(
    id serial constraint dm_users_pkey primary key,
    user_id varchar not null,
    user_name varchar not null,
    user_login varchar not null
); 

-- Двигайтесь дальше! Ваш код: g0MSs4ez0v
