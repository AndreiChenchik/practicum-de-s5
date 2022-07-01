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

-- Двигайтесь дальше! Ваш код: Ve7J48uY2K
