{{ config(
   materialized = 'incremental',
   tag = ['dimension'],
   unique_key = 'promo_code',
   pre_hook= 'create sequence if not exists promo_code as int
   start 1
   increment 1;'
)
}}


with source as(
select   
    "promo_code",
    "promo_name",
    "promo_subcategory",
    "promo_category",
    "promo_cost",
    "promo_begin_date",
    "promo_end_date",
    "promo_total",
    "insert_dt",
    "last_update_dt"
from  {{ref('stg_promo')}}
),


sq_promo as(
select 
    "promo_code",
    "promo_name",
    "promo_subcategory",
    "promo_category",
    "promo_cost",
    "promo_begin_date",
    "promo_end_date",
    "promo_total",
    "insert_dt",
    "last_update_dt"
    from source
),

{% if is_incremental() %}
lkp_dim_promo as(
select 0 as "promo_key",
           '-' as "promo_code"       
    union
    select 
    d."promo_key",
    d."promo_code"
    from {{this}} d
),

router_trans_update as(
    select 
    d."promo_key",
    s."promo_code",
    s."promo_name",
    s."promo_subcategory",
    s."promo_category",
    s."promo_cost",
    s."promo_begin_date",
    s."promo_end_date",
    s."promo_total",
    s."insert_dt",
    s."last_update_dt" from sq_promo s left join 
    lkp_dim_promo d  on  d."promo_code" = s."promo_code"
    where d."promo_key" is not null
),


{% endif %}
-- exptrans as(
-- select distinct
-- lkp_dim_promo.promo_code,
-- current_timestamp as "systemdate"
-- from lkp_dim_promo
-- ),

-- to change from here
router_trans_insert as(
select 
    s."promo_code",
    s."promo_name",
    s."promo_subcategory",
    s."promo_category",
    s."promo_cost",
    s."promo_begin_date",
    s."promo_end_date",
    s."promo_total",
    s."insert_dt",
    s."last_update_dt" from sq_promo s 
    {% if is_incremental() %}
    left join 
    lkp_dim_promo d  on  d."promo_code" = s."promo_code"
    where d."promo_key" is null
    {% endif %}
),



upd_insert_dim_promotions as(
    select
       -- "systemdate",
        nextval('promo_code') as "promo_key",
        "promo_code",
        "promo_name",
        "promo_subcategory",
        "promo_category",
        "promo_cost",
        "promo_begin_date",
        "promo_end_date",
        "promo_total",
        "insert_dt",
        "last_update_dt"
        from router_trans_insert 
),

{% if is_incremental() %}
upd_update_dim_promotions as(
     select
       -- "systemdate",
        "promo_key",
        "promo_code",
        "promo_name",
        "promo_subcategory",
        "promo_category",
        "promo_cost",
        "promo_begin_date",
        "promo_end_date",
        "promo_total",
        "insert_dt",
        "last_update_dt"
        from router_trans_update 
),
{% endif %}

dim_promotions as(
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        {{dbt_utils.surrogate_key(("promo_code"))}} as {{"md5_checksum"}},
        'I' as "cdc_flag"
        from  
        upd_insert_dim_promotions
        {% if is_incremental() %}
        union all
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        {{dbt_utils.surrogate_key(("promo_code"))}} as {{"md5_checksum"}},
        'U' as "cdc_flag"
        from  
        upd_update_dim_promotions
        {% endif %}
)

select * from dim_promotions


