{{
    config(
        materialized = "incremental",
        tag = ['fact'],
        pre_hook = {"sql" : "create sequence if not exists order_number as int
                        start 1
                        increment 1;
                 "}
    )
}}

with source as (
    select 
        "order_number",
        "prod_code",
        "cust_code",
        "sale_date",
        "channel_code",
        "promo_code",
        "quantity_sold",
        "amount_sold",
        "insert_dt",
        "last_update_dt"
        from {{ref ('stg_sales') }}
),

sq_sales as (
    select 
        "order_number",
        "prod_code",
        "cust_code",
        "sale_date",
        "channel_code",
        "promo_code",
        "quantity_sold",
        "amount_sold",
        "insert_dt",
        "last_update_dt"
        from source
),

lkp_dim_products as (
    select 
    d."prod_key",
    d."prod_code",
    s."prod_code" as "in_prod_code"
    from sq_sales s 
    left outer join {{ref ('dim_products')}} d on s."prod_code" = d."prod_code"
),

lkp_dim_customers as (
    select 
    d."cust_key",
    d."cust_code",
    s."cust_code" as "in_cust_code"
    from sq_sales s 
    left outer join {{ref ('dim_customer')}} d on s."cust_code" = d."cust_code"
),

lkp_promotions as (
    select 
    d."promo_key",
    d."promo_code",
    s."promo_code" as "in_promo_code"
    from sq_sales s 
    left outer join {{ref ('dim_promo')}} d on s."promo_code" = d."promo_code"
),

lkp_dim_channels as(
    select
    d."channel_key",
    d."channel_code",
    s."channel_code" as "in_channel_code"
    from sq_sales s 
    left outer join {{ref ('dim_channel')}} d on s."channel_code" = d."channel_code"
),
exp_curr_date as(
    select 
    current_timestamp as CurrDate,
    
)
{% if is_incremental() %}
, lkp_fact_sales as(
    select 
        s."order_number" 
    from sq_sales d
    left outer join {{this}} s  on s."order_number" = d."order_number"
),
rtr_update_update_fact_sales as(
    select
    l."order_number" as "lkp_order_number",
    s."order_number" as "src_order_number",
    p."prod_key",
    c."cust_key",
    ch."channel_key",
    po."promo_key",
    s."quantity_sold",
    s."amount_sold",
    s."insert_dt",
    s."last_update_dt"
    from sq_sales s
    left join lkp_fact_sales l on l."order_number" = s."order_number"
    left join lkp_dim_products p on p."prod_code" = s."prod_code"
    left join lkp_dim_customers c on c."cust_code" = s."cust_code"
    left join lkp_dim_channels ch on ch."channel_code" = s."channel_code"
    left join lkp_promotions po on po."promo_code" = s."promo_code"
    where s."order_number" is not null
),

{% endif %}


rtr_update_insert_fact_sales as(
    select
    s."order_number" as "src_order_number",
    p."prod_key",
    c."cust_key",
    ch."channel_key",
    po."promo_key",
    s."quantity_sold",
    s."amount_sold",
    s."insert_dt",
    s."last_update_dt"
    from sq_sales s
    {% if is_incremental() %}
    left join lkp_fact_sales l on l."order_number" = s."order_number"
    {% endif %}
    left join lkp_dim_products p on p."prod_code" = s."prod_code"
    left join lkp_dim_customers c on c."cust_code" = s."cust_code"
    left join lkp_dim_channels ch on ch."channel_code" = s."channel_code"
    left join lkp_promotions po on po."promo_code" = s."promo_code"
    {% if is_incremental() %}
    where l."order_number" is null
    {% endif %}
),


update_strategy_insert as(
    select
        nextval('order_number') as "sale_date_time_key",
        "src_order_number",
        "prod_key",
        "cust_key",
        "channel_key",
        "promo_key",
        "quantity_sold",
        "amount_sold",
        "insert_dt",
        "last_update_dt"
        from rtr_update_insert_fact_sales
),

update_strategy_update as(
    select
        nextval('order_number') as "sale_date_time_key",
        "lkp_order_number",
        "prod_key",
        "cust_key",
        "channel_key",
        "promo_key",
        "quantity_sold",
        "amount_sold",
        "insert_dt",
        "last_update_dt"
        from rtr_update_update_fact_sales
),

fact_sales as(
    select *,
    current_timestamp as "dw_insert_dt",
    current_timestamp as "dw_update_dt",
    concat("lkp_order_number","prod_key") as "md5_checksum",
    case when "lkp_order_number" is not null then 'update'
    end as "cdc_flag"
    from     
    update_strategy_update
    union all
    select *,
    current_timestamp as "dw_insert_dt",
    current_timestamp as "dw_update_dt",
    concat("src_order_number","prod_key") as "md5_checksum",
    case when "src_order_number" is null then 'insert'
    end as "cdc_flag"
    from     
    update_strategy_insert
)

select * from fact_sales
