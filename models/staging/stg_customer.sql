{{config(
    materialized = 'incremental',
    unique_key = 'customer_id',

    )
}}

with customers as (
    select * from 
    {{ref('customer')}}
),

incremental as (
    select  full_name, customer_id,date_of_birth,current_date
    from customers s
    {% if is_incremental() %}
    where customer_id not in (select customer_id from {{this}})
    {% endif %}
)

select * from incremental



-- incremental as ( 
--     select 
--         *, 
--         current_timestamp as current_date
--     from customers
--     {% if is_incremental() %}
--      where current_date > (select max(current_date) from {{this}})
--     {% endif %} 
-- )
