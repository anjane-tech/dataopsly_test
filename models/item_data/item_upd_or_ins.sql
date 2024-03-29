{{
    config(
        materialized = 'incremental',
        tags = ['item_data'],
        unique_key = 'item_id',
        pre_hook = {"sql" : "create sequence if not exists item_id as int
                        start 1
                        increment 1;
                 "}
    )
}}

with sq_item as(
    select
    item_id,
    price,
    discount,
    bonus_flag
    from {{ref('stg_item')}}
),

{{% if is_incremental() %}}
lookup_procedure as (
    select 
    item_id,
    item_key,
    price,
    discount,
    bonus_flag
    from {{this}}
),
{{% endif %}}

exp_compare_current as (
    select 
    s.item_id,
    s.price,
    s.discount,
    s.bonus_flag,
    {{% if is_incremental() %}}
    l.item_id as prev_item_id,
    l.item_key as prev_item_key,
    l.price as prev_price,
    l.discount as prev_discount,
    l.bonus_flag as prev_bonus_flag,
    case when prev_item_key is not null 
    then 0 
    end as v_update_flag,
    case 
    when v_update_flag = true then 'update'
    {{% endif %}}
    from sq_item s
    {{% if is_incremental() %}}
    left join lookup_procedure l on l.item_id = s.item_id
    {{% endif %}}
),

