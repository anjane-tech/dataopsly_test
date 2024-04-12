{{
  config(
    materialized='incremental',
    unique_key = 'unique_prod_id',
    tags = ["dimensions"]
  )
}}

with products as (
    SELECT * 
    FROM {{ref('stg_products')}}
),

orders as (
    SELECT * 
    FROM {{ref('stg_order')}}
),

dimension as (
    SELECT DISTINCT
            COALESCE (nullif(trim(sp.product_name),''), 'N/A') AS product_name,
            CASE WHEN sp.product_id IS NOT NULL THEN SP.product_id
                WHEN so.product_id IS NOT NULL THEN SO.product_id
                ELSE NULL END AS product_id,
            COALESCE (sp.price, 0) AS price,
            COALESCE (nullif(trim(sp.product_description),''), 'N/A') AS product_description,
            CASE WHEN sp.vendor_id IS NOT NULL THEN SP.vendor_id
                WHEN so.vendor_id IS NOT NULL THEN SO.vendor_id
                ELSE NULL END AS vendor_id,
            CASE WHEN sp.suit_number IS NOT NULL THEN SP.suit_number
                WHEN so.suit_number IS NOT NULL THEN SO.suit_number
                ELSE NULL END AS suit_number,
            current_timestamp as current_date
    FROM products sp
    FULL OUTER JOIN orders so ON so.product_id = sp.product_id      
)

{{ surrogate_key('product_id','unique_prod_id') }}