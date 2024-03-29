{{ config(
   materialized = 'incremental',
   tag = ['dimension'],
   unique_key = 'prod_code',
   pre_hook= 'create sequence if not exists prod_code as int
   start 1
   increment 1;'
   
)
}}

with source as(
select   
    "prod_code",
    "prod_name",
    "prod_desc",
    "prod_subcategory",
    "prod_subcategory_desc",
    "prod_category",
    "prod_category_desc",
    "prod_weight_class",
    "prod_unit_of_measure",
    "prod_pack_size",
    "prod_status",
    "prod_list_price",
    "prod_min_price",
    "prod_total",
    "insert_dt",
    "last_update_dt",
    "prod_valid"
from  {{ref('stg_products')}}
),


sq_products as(
select 
        "prod_code",
        "prod_name",
        "prod_desc",
        "prod_subcategory",
        "prod_subcategory_desc",
        "prod_category",
        "prod_category_desc",
        "prod_weight_class",
        "prod_unit_of_measure",
        "prod_pack_size",
        "prod_status",
        "prod_list_price",
        "prod_min_price",
        "prod_total",
        "insert_dt",
        "last_update_dt",
        "prod_valid" from source
),

{% if is_incremental() %}
lkp_dim_products as(
        select 0 as "prod_key",
           '-' as "prod_code"
        union
        select  
        d."prod_key",
        d."prod_code"
        from {{this}} d 
),

router_trans_update as( 
select  d."prod_key",
        s."prod_code",
        s."prod_name",
        s."prod_desc",
        s."prod_subcategory",
        s."prod_subcategory_desc",
        s."prod_category",
        s."prod_category_desc",
        s."prod_weight_class",
        s."prod_unit_of_measure",
        s."prod_pack_size",
        s."prod_status",
        s."prod_list_price",
        s."prod_min_price",
        s."prod_total",
        s."insert_dt",
        s."last_update_dt",
        s."prod_valid" from sq_products s 
        left join lkp_dim_products d  on  d."prod_code" = s."prod_code"
        where d."prod_key" is not null
),
{% endif %}



-- exptrans as(
-- select distinct
-- lkp_dim_products."prod_code",
-- current_timestamp as "systemdate"
-- from lkp_dim_products
-- ),

router_trans_insert as(
select 
        s."prod_code",
        s."prod_name",
        s."prod_desc",
        s."prod_subcategory",
        s."prod_subcategory_desc",
        s."prod_category",
        s."prod_category_desc",
        s."prod_weight_class",
        s."prod_unit_of_measure",
        s."prod_pack_size",
        s."prod_status",
        s."prod_list_price",
        s."prod_min_price",
        s."prod_total",
        s."insert_dt",
        s."last_update_dt",
        s."prod_valid" from sq_products s 
        {% if is_incremental() %}
        left join 
        lkp_dim_products d  on  d."prod_code" = s."prod_code"
        where d."prod_key" is null
        {% endif %}
),



upd_insert_dim_products as(
    select
       -- "systemdate",
        nextval('prod_code') as "prod_key",
        "prod_code",
        "prod_name",
        "prod_desc",
        "prod_subcategory",
        "prod_subcategory_desc",
        "prod_category",
        "prod_category_desc",
        "prod_weight_class",
        "prod_unit_of_measure",
        "prod_pack_size",
        "prod_status",
        "prod_list_price",
        "prod_min_price",
        "prod_total",
        "insert_dt",
        "last_update_dt",
        "prod_valid"
        from router_trans_insert 
),

{% if is_incremental() %}
upd_update_dim_products as(
    select
        "prod_key",
        "prod_code",
        "prod_name",
        "prod_desc",
        "prod_subcategory",
        "prod_subcategory_desc",
        "prod_category",
        "prod_category_desc",
        "prod_weight_class",
        "prod_unit_of_measure",
        "prod_pack_size",
        "prod_status",
        "prod_list_price",
        "prod_min_price",
        "prod_total",
        "insert_dt",
        "last_update_dt",
        "prod_valid"
        from router_trans_update 
),
{% endif %}

dim_products as(
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        {{dbt_utils.surrogate_key(("prod_code"))}} as {{"md5_checksum"}},
        'I' as "cdc_flag",
        current_timestamp as "prod_eff_from", 
        current_timestamp as "prod_eff_to"
        from  
        upd_insert_dim_products
        {% if is_incremental() %}
        union all
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        {{dbt_utils.surrogate_key(("prod_code"))}} as {{"md5_checksum"}},
        'U' as "cdc_flag",
        current_timestamp as "prod_eff_from", 
        current_timestamp as "prod_eff_to"
        from  
        upd_update_dim_products
        {% endif %}
)

select * from dim_products