def load_dm_settlement_report(conn_hook):
    conn = conn_hook.get_conn()
    cur = conn.cursor()

    sql = """
        with fct_product_sales as (
            select distinct 
                order_id, total_sum, bonus_payment, bonus_grant 
            from dds.fct_product_sales
        )

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
                count(fps.order_id)                                      as orders_count,
                sum(fps.total_sum)                                  as orders_total_sum,
                sum(fps.bonus_payment)                              as orders_bonus_payment_sum,
                sum(fps.bonus_grant)                                as orders_bonus_granted_sum,
                sum(fps.total_sum) * 0.25                           as order_processing_fee,
                sum(fps.total_sum) * 0.75 - sum(fps.bonus_payment)  as restaurant_reward_sum
            from fct_product_sales fps
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
    """

    cur.execute(sql)
    conn.commit()
