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
        restaurant_id, settlement_year, settlement_month
    )
;

-- Двигайтесь дальше! Ваш код: ZBfLxNLryk
