{{
    config(
        materialized = "incremental",
        unique_key = "order_number",
        tag = ['dimension']
    )
}}

with sq_sales as (
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

lkp_dim_products as (
    select 
    d."prod_key",
    d."prod_code"
    from {{ref ('dim_products')}} d
),

lkp_dim_customers as (
    select 
    d."cust_key",
    d."cust_code"
    from {{ref ('dim_customer')}} d
),

lkp_promotions as (
    select 
    d."promo_key",
    d."promo_code"
    from {{ref ('dim_promo')}} d
),

lkp_dim_channels as(
    select
    d."channel_key",
    d."channel_code"
    from {{ref ('dim_channel')}} d
),
-- exp_curr_date as(
--     select 
--     current_timestamp as "currdate",
--     to_char(sale_date,'MON-DD-YYYY HH12:MIPM') as datetime
--     from sq_sales 
-- ),
{% if is_incremental() %}
 lkp_fact_sales as(
    select 
        s."order_number" 
    from {{this}} s
),

rtr_update_update_fact_sales as(
    select
    current_timestamp as "currdate",
    to_char(s.sale_date,'MON-DD-YYYY HH:MI') as "datetime",
    s."order_number",
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
    where l."order_number" is not null
),

{% endif %}


rtr_update_insert_fact_sales as(
    select
    current_timestamp as "currdate",
    to_char(s.sale_date,'MON-DD-YYYY HH:MI') as "datetime",
    s."order_number" ,
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
        "currdate",
        "datetime",
        "order_number",
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
{% if is_incremental() %}
update_strategy_update as(
    select
        "currdate",
        "datetime",
        "order_number",
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
{% endif %}

fact_sales as(
    {% if is_incremental() %}
    select 
    "datetime" as "sale_date_time_key",
    "order_number",
    "prod_key",
    "cust_key",
    "channel_key",
    "promo_key",
    "quantity_sold",
    "amount_sold",
    "insert_dt",
    "last_update_dt",
    "currdate" as "dw_insert_dt",
    current_timestamp as "dw_update_dt",
    concat("order_number","prod_key") as "md5_checksum",
    'U' as "cdc_flag"
    from update_strategy_update
    union all
    {% endif %}
    select 
    "datetime" as "sale_date_time_key",
    "order_number",
    "prod_key",
    "cust_key",
    "channel_key",
    "promo_key",
    "quantity_sold",
    "amount_sold",
    "insert_dt",
    "last_update_dt",
    "currdate" as "dw_insert_dt",
    current_timestamp as "dw_update_dt",
    concat("order_number","prod_key") as "md5_checksum",
    'I' as "cdc_flag"
    from     
    update_strategy_insert
)

select * from fact_sales
