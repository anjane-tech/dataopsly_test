{{
    config(
        materialized = 'table'
    )
}}

select * from {{source('infor_dbt_item', 'item_data')}}