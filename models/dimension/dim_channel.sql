{{config(
    materialized= 'incremental',
    tags = ['dimension'],    
    unique_key = 'channel_code',
    merge_update_columns = ['channel_desc'],
    pre_hook = {"sql" : "create sequence if not exists channel_code as int
                        start 1
                        increment 1;
                 "}
)}}


with source as (
select 
        "channel_code",
        "channel_desc",
        "channel_class",
        "insert_dt",
        "last_update_dt"
    from {{ref('stg_channel')}}
),

source_qualifier as (
    select distinct
        "channel_code",
        "channel_desc",
        "channel_class",
        "insert_dt",
        "last_update_dt"
    from source
),

{% if is_incremental() %}
lookup_procedure as (
    select 
    0 as "channel_key",
    '-' as "channel_code"
    union
    select
    d."channel_key",
    d."channel_code"
    from {{this}} d    
),

router_update as (  
        select 
            l."channel_key" as "channel_key1",
            s."channel_code" as "channel_code1",
            s."channel_desc" as "channel_desc1",
            s."channel_class" as "channel_class1",
            s."insert_dt" as "insert_dt1",
            s."last_update_dt" as "last_update_dt1"
        from source_qualifier s
        left join lookup_procedure l on s."channel_code" = l."channel_code"
        where l."channel_key" is not null   
),
{% endif %}

router_insert as (
    select 
        s."channel_code" as "channel_code3",
        s."channel_desc" as "channel_desc3",
        s."channel_class" as "channel_class3",
        s."insert_dt" as "insert_dt3",
        s."last_update_dt" as "last_update_dt3"
    from source_qualifier s
    {% if is_incremental() %}
    left join 
    lookup_procedure l on s."channel_code" = l."channel_code"
    where l."channel_key" is null
    {% endif %}
),


-- sequence_col as (
--     select nextval('channel_code') as "nextval",
--         currval('channel_code') as "currval"
-- ),

--     expression as(
--         select
--             current_timestamp as "systimestamp",
--             "in_channel_code"
--         from lookup_procedure
-- ),

update_stratagy_ins as (
        select 
            nextval('channel_code') as "channel_key",
            "channel_code3" as "channel_code",
            "channel_desc3" as "channel_desc",
            "channel_class3" as "channel_class",
            "insert_dt3" as "insert_dt",
            "last_update_dt3" as "last_update_dt"
        from router_insert
),

{% if is_incremental() %}
update_stratagy_upd as (
        select 
            "channel_key1" as "channel_key",
            "channel_code1" as "channel_code",
            "channel_desc1" as "channel_desc",
            "channel_class1" as "channel_class",
            "insert_dt1" as "insert_dt",
            "last_update_dt1" as "last_update_dt"
        from router_update
),
{% endif %}    

    
dim_channel as (
        select *,
            current_timestamp as "dw_insert_dt",
            current_timestamp as "dw_update_dt",
            {{dbt_utils.surrogate_key(("channel_code"))}} as {{"md5_checksum"}},
            'I' as "cdc_flag",
            current_timestamp as "channel_eff_from",
            current_timestamp as "channel_eff_to"
        from update_stratagy_ins
        {% if is_incremental() %}
        union all
        select *,
            current_timestamp as "dw_insert_dt",
            current_timestamp as "dw_update_dt",
            {{dbt_utils.surrogate_key(("channel_code"))}} as {{"md5_checksum"}},
            'U' as "cdc_flag",
            current_timestamp as "channel_eff_from",
            current_timestamp as "channel_eff_to"                    
        from update_stratagy_upd
        {% endif %}
)
    
select * from dim_channel