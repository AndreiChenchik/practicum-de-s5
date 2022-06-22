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

-- Двигайтесь дальше! Ваш код: S3Hlgxf3Vd
