{{
    config(
        materialized = 'incremental',
        tag = ['dimension'],    
        unique_key = 'country_code',
        pre_hook ={
            "sql": "create sequence if not exists country_code as int
            increment 1
            start 1;"
        }
    )
}}

with source as(
    select 
        "country_code",
        "country_iso_code",
        "country_name",
        "country_subregion",
        "country_region",
        "country_total",
        "insert_dt",
        "last_update_dt"
    from {{ref('stg_countries')}}    
),

sq_countries as (
     select 
        "country_code",
        "country_iso_code",
        "country_name",
        "country_subregion",
        "country_region",
        "country_total",
        "insert_dt",
        "last_update_dt"
    from source
),

{% if is_incremental() %}
lkp_dim_coun as(
    select 0 as "country_key",
           '-' as "country_code" 
    union
    select
    d."country_key",
    d."country_code"
    from {{this}} d 
),

router_trans_update as(
    select
        d."country_key",
        s."country_code",
        s."country_iso_code",
        s."country_name",
        s."country_subregion",
        s."country_region",
        s."country_total",
        s."insert_dt",
        s."last_update_dt"
        from sq_countries s 
        left join lkp_dim_coun d on d."country_code" = s."country_code"
        where d."country_key" is not null
),
{% endif %}

-- exp_trans as(
--     select distinct
--     current_timestamp as "systemdate"
--     from lkp_dim_coun
-- ),

router_trans_insert as(
    select
        s."country_code",
        s."country_iso_code",
        s."country_name",
        s."country_subregion",
        s."country_region",
        s."country_total",
        s."insert_dt",
        s."last_update_dt"
        from sq_countries s 
        {% if is_incremental() %}
        left join       
        lkp_dim_coun d on d."country_code" = s."country_code"
        where d."country_key" is null
        {% endif %}
),

upd_insert_dim_countries as(
     select 
        nextval('country_code') as "country_key",
        "country_code",
        "country_iso_code",
        "country_name",
        "country_subregion",
        "country_region",
        "country_total",
        "insert_dt",
        "last_update_dt"
        from 
        router_trans_insert
),


{% if is_incremental() %}
upd_update_dim_countries as(
     select 
        "country_key",
        "country_code",
        "country_iso_code",
        "country_name",
        "country_subregion",
        "country_region",
        "country_total",
        "insert_dt",
        "last_update_dt"
        from 
        router_trans_update
),
{% endif %}

dim_countries as(
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        {{dbt_utils.surrogate_key(("country_code"))}} as {{"md5_checksum"}},
        'I' as "cdc_flag",
        current_timestamp as "cust_eff_from", 
        current_timestamp as "cust_eff_to"
        from  
        upd_insert_dim_countries
        {% if is_incremental() %}
        union all
        select 
        *,
        current_timestamp as "dw_insert_dt",
        current_timestamp as "dw_update_dt",
        {{dbt_utils.surrogate_key(("country_code"))}} as {{"md5_checksum"}},
        'U' as "cdc_flag",
        current_timestamp as "cust_eff_from", 
        current_timestamp as "cust_eff_to"
        from  
        upd_update_dim_countries
        {% endif %}
)

select * from dim_countries

