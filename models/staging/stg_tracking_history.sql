{{config(
    materialized = 'table'
)
}}

select * from {{source('infor_dbt_dimensions','tracking_history')}}