drop schema if exists cdm cascade;
create schema cdm;

create table if not exists cdm.dm_settlement_report (
    id serial not null,
    restaurant_id integer not null,
    restaurant_name varchar not null,
    settlement_date date not null check(settlement_date > '2022-01-01' AND settlement_date < '2050-01-01'),
    settlement_year smallint not null,
    settlement_month smallint not null,
    orders_count integer not null,
    orders_total_sum numeric(14, 2) not null,
    orders_bonus_payment_sum numeric(14, 2) not null,
    orders_bonus_granted_sum numeric(14, 2) not null,
    order_processing_fee numeric(14, 2) not null,
    restaurant_reward_sum numeric(14, 2) not null
);

alter table cdm.dm_settlement_report
    drop constraint if exists pk_id,
    add constraint pk_id primary key (id),   

    alter column orders_count drop default,
    alter column orders_count set default 0,
    drop constraint if exists orders_count_range,
    add constraint orders_count_range check (
        orders_count >= 0
    ),
    
    alter column orders_total_sum drop default,
    alter column orders_total_sum set default 0,
    drop constraint if exists orders_total_sum_range,
    add constraint orders_total_sum_range check (
        orders_total_sum >= 0
    ),

    alter column orders_bonus_payment_sum drop default,
    alter column orders_bonus_payment_sum set default 0,
    drop constraint if exists orders_bonus_payment_sum_range,
    add constraint orders_bonus_payment_sum_range check (
        orders_bonus_payment_sum >= 0
    ),

    alter column orders_bonus_granted_sum drop default,
    alter column orders_bonus_granted_sum set default 0,
    drop constraint if exists orders_bonus_granted_sum_range,
    add constraint orders_bonus_granted_sum_range check (
        orders_bonus_granted_sum >= 0
    ),

    alter column restaurant_reward_sum drop default,
    alter column restaurant_reward_sum set default 0,
    drop constraint if exists restaurant_reward_sum_range,
    add constraint restaurant_reward_sum_range check (
        restaurant_reward_sum >= 0
    ),

    alter column order_processing_fee drop default,
    alter column order_processing_fee set default 0,
    drop constraint if exists order_processing_fee_range,
    add constraint order_processing_fee_range check (
        order_processing_fee >= 0
    ),

    drop constraint if exists settlement_year_range,
    add constraint settlement_year_range check ( 
        settlement_year >= 2022 and settlement_year < 2500
    ),

    drop constraint if exists settlement_month_range,
    add constraint settlement_month_range check (
        settlement_month >= 1 and settlement_month <= 12
    ),

    drop constraint if exists unique_rid_syear_smonth,
    add constraint unique_rid_syear_smonth unique (
        restaurant_id, settlement_date, settlement_year, settlement_month
    )
;


DELETE FROM dds.dm_products;
DELETE FROM dds.dm_orders;
DELETE FROM dds.dm_users;
DELETE FROM dds.dm_timestamps;
DELETE FROM dds.dm_restaurants;
DELETE FROM dds.fct_product_sales;
DELETE FROM cdm.dm_settlement_report;

COPY dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to) FROM '/data/dm_restaurants_';
COPY dds.dm_products (id, restaurant_id, product_id, product_name, product_price, active_from, active_to) FROM '/data/dm_products_';
COPY dds.dm_timestamps (id, ts, year, month, day, "time", date) FROM '/data/dm_timestamps_';
COPY dds.dm_users (id, user_id, user_name, user_login) FROM '/data/dm_users_';
COPY dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id) FROM '/data/dm_orders_';
COPY dds.fct_product_sales (id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant) FROM '/data/fct_product_sales_';

insert into cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date, 
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
    select
        dmr.id                                              as restaurant_id,
        dmr.restaurant_name                                 as restaurant_name,
        dmt.date                                            as settlement_date, 
        dmt.year                                            as settlement_year,
        dmt.month                                           as settlement_month,
        sum(fps.count)                                      as orders_count,
        sum(fps.total_sum)                                  as orders_total_sum,
        sum(fps.bonus_payment)                              as orders_bonus_payment_sum,
        sum(fps.bonus_grant)                                as orders_bonus_granted_sum,
        sum(fps.total_sum) * 0.25                           as order_processing_fee,
        sum(fps.total_sum) * 0.75 - sum(fps.bonus_payment)  as restaurant_reward_sum
    from dds.fct_product_sales fps
    left join dds.dm_orders dmo
        on fps.order_id = dmo.id
    left join dds.dm_restaurants dmr 
        on dmo.restaurant_id = dmr.id
    left join dds.dm_timestamps dmt
        on dmo.timestamp_id = dmt.id
    where dmo.order_status='CLOSED'
    group by 
        dmr.restaurant_name,
        dmr.id,
        dmt.year,
        dmt.month,
        dmt.date
on conflict 
    (restaurant_id, settlement_date, settlement_year, settlement_month)
    do update set (
        orders_count,
        orders_total_sum,
        orders_bonus_payment_sum,
        orders_bonus_granted_sum,
        order_processing_fee,
        restaurant_reward_sum
    ) = (
        excluded.orders_count,
        excluded.orders_total_sum,
        excluded.orders_bonus_payment_sum,
        excluded.orders_bonus_granted_sum,
        excluded.order_processing_fee,
        excluded.restaurant_reward_sum
    )
    
-- Двигайтесь дальше! Ваш код: mgXgcqQzFv
