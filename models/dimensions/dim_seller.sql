{{
    config(
        materialized = 'incremental',
        unique_key = 'unique_seller_id',
        tags = ["dimensions"]
    )
}}

with seller as(
    select * 
    from {{ref('stg_seller')}} 
),

orders as(
    select * 
    from {{ref('stg_order')}}
),

dimension as(
    select DISTINCT
            CASE WHEN ss.suit_number IS NOT NULL THEN SS.suit_number
                WHEN so.suit_number IS NOT NULL THEN SO.suit_number
                ELSE NULL END AS suit_number,
            COALESCE (nullif(trim(ss.seller_name),''), 'N/A') AS seller_name,
            CASE WHEN ss.product_id IS NOT NULL THEN SS.product_id
                WHEN so.product_id IS NOT NULL THEN SO.product_id
                ELSE NULL END AS product_id,
            COALESCE (ss.seller_amount,0) AS seller_amount,
            CASE WHEN ss.order_id IS NOT NULL THEN SS.order_id
                WHEN so.order_id IS NOT NULL THEN SO.order_id
                ELSE NULL END AS order_id,
            COALESCE (nullif(trim(ss.platform),''), 'N/A') AS platform,
            current_timestamp as current_date
            FROM seller ss
            FULL OUTER JOIN orders so on so.suit_number = ss.suit_number
)

{{ surrogate_key('suit_number','unique_seller_id') }}