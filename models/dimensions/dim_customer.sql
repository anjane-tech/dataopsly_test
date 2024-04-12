{{
    config(
        materialized = 'incremental',
        unique_key = 'unique_cust_id',
        tags = ["dimensions"]
    )
}}

with customer as(
    select * 
    from {{ref('stg_customer')}}
),

orders as (
    select * 
    from {{ref('stg_order')}}  
),

dimension as (
    select DISTINCT
            COALESCE (nullif(trim(sc.full_name),''), 'N/A') as full_name,
            CASE WHEN sc.customer_id IS NOT NULL THEN SC.customer_id
                WHEN so.customer_id IS NOT NULL THEN SO.customer_id
                ELSE NULL END AS customer_id,
            COALESCE (sc.date_of_birth, '1990-01-01') as date_of_birth,
            current_timestamp as current_date
            from customer sc 
            left JOIN orders so on so.customer_id = sc.customer_id
)

{{ surrogate_key('customer_id','unique_cust_id') }}


 
