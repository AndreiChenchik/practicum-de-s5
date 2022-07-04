drop schema if exists stg cascade;
create schema stg;

CREATE TABLE
    stg.bonussystem_users (
        id integer NOT NULL,
        order_user_id text NOT NULL
    );

ALTER TABLE
    stg.bonussystem_users
ADD
    CONSTRAINT users_pkey PRIMARY KEY (id);

CREATE TABLE
    stg.bonussystem_ranks (
        id integer NOT NULL,
        name character varying(2048) NOT NULL,
        bonus_percent numeric(19, 5) NOT NULL,
        min_payment_threshold numeric(19, 5) NOT NULL
    );

ALTER TABLE
    stg.bonussystem_ranks
ADD
    CONSTRAINT ranks_pkey PRIMARY KEY (id);


CREATE TABLE
    stg.bonussystem_events (
        id integer NOT NULL,
        event_ts timestamp without time zone NOT NULL,
        event_type character varying NOT NULL,
        event_value text NOT NULL
    );

ALTER TABLE
    stg.bonussystem_events
ADD
    CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id);

create index idx_bonussystem_events__event_ts 
    ON stg.bonussystem_events (event_ts);

drop table if exists stg.ordersystem_users;
CREATE TABLE stg.ordersystem_users
(
    id serial CONSTRAINT ordersystem_users_pkey PRIMARY KEY,
    object_id varchar NOT NULL CONSTRAINT ordersystem_users_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp not null
); 

drop table if exists stg.ordersystem_restaurants;
CREATE TABLE stg.ordersystem_restaurants
(
    id serial CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY,
    object_id varchar NOT NULL CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp not null
); 

drop table if exists stg.ordersystem_orders;
CREATE TABLE stg.ordersystem_orders
(
    id serial CONSTRAINT ordersystem_orders_pkey PRIMARY KEY,
    object_id varchar NOT NULL CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp not null
); 

drop schema if exists dds cascade;
create schema dds;

drop table if exists dds.dm_users;
create table dds.dm_users
(
    id serial constraint dm_users_pkey primary key,
    user_id varchar not null,
    user_name varchar not null,
    user_login varchar not null
); 

drop table if exists dds.dm_restaurants;
create table dds.dm_restaurants
(
    id serial constraint dm_restaurants_pkey primary key,
    restaurant_id varchar not null,
    restaurant_name varchar not null,
    active_from timestamp not null,
    active_to timestamp not null
); 

drop table if exists dds.dm_products;
create table dds.dm_products
(
    id serial constraint dm_products_pkey primary key,
    product_id varchar not null,
    product_name varchar not null,
    product_price numeric(14, 2) not null default 0
        constraint dm_products_price_check check (product_price >= 0),
    active_from timestamp not null,
    active_to timestamp not null,
    restaurant_id integer not null
); 

alter table dds.dm_products
    add constraint dm_products_restaurant_id_fkey 
        foreign key (restaurant_id) references dds.dm_restaurants (id);

drop table if exists dds.dm_timestamps;
create table dds.dm_timestamps
(
    id serial constraint dm_timestamps_pkey primary key,
    ts timestamp not null,
    year integer not null 
        constraint dm_timestamps_year_check check (year >= 2022 and year < 2500),
    month integer not null
        constraint dm_timestamps_month_check check (month >= 1 and month <= 12),
    day integer not null
        constraint dm_timestamps_day_check check (day >= 1 and day <= 31),
    time time not null,
    date date not null
); 

drop table if exists dds.dm_orders;
create table dds.dm_orders
(
    id serial constraint dm_orders_pkey primary key,
    order_key varchar not null unique,
    order_status varchar not null,
    user_id integer not null,
    restaurant_id integer not null,
    timestamp_id integer not null
); 

alter table dds.dm_orders
    add constraint dm_orders_restaurant_id_fkey 
        foreign key (restaurant_id) references dds.dm_restaurants (id) on delete cascade,
    add constraint dm_orders_user_id_fkey 
        foreign key (user_id) references dds.dm_users (id) on delete cascade,
    add constraint dm_orders_timestamp_id_fkey 
        foreign key (timestamp_id) references dds.dm_timestamps (id) on delete cascade;

drop table if exists dds.fct_product_sales;
create table dds.fct_product_sales
(
    id serial constraint fct_product_sales_pkey primary key,
    product_id integer not null,
    order_id integer not null,
    count integer not null default 0
        constraint fct_product_sales_count_check check (count >= 0),
    price numeric(14, 2) not null default 0
        constraint fct_product_sales_price_check check (price >= 0),
    total_sum numeric(14, 2) not null default 0
        constraint fct_product_sales_total_sum_check check (total_sum >= 0),
    bonus_payment numeric(14, 2) not null default 0
        constraint fct_product_sales_bonus_payment_check check (bonus_payment >= 0),
    bonus_grant numeric(14, 2) not null default 0
        constraint fct_product_sales_bonus_grant_check check (bonus_grant >= 0)
); 

alter table dds.fct_product_sales
    add constraint fct_product_sales_product_id_fkey 
        foreign key (product_id) references dds.dm_products (id) on delete cascade,
    add constraint fct_product_sales_order_id_fkey 
        foreign key (order_id) references dds.dm_orders (id) on delete cascade;

create table stg.srv_wf_settings (
	id serial primary key,
	workflow_key varchar,
	workflow_settings text
);

create table dds.srv_wf_settings (
	id serial primary key,
	workflow_key varchar,
	workflow_settings text
);
