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
        dmt.year                                            as settlement_year,
        dmt.month                                           as settlement_month,
        count(distinct fps.order_id)                        as orders_count,
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
        dmt.month
on conflict 
    (restaurant_id, settlement_year, settlement_month)
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
    
-- 
