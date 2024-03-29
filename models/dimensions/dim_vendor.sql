{{
    config(
        materialized = 'incremental',
        unique_key = "'unique_vendor_id'",
        tags = ["dimensions"]
    )
}}

with vendor as(
    select * 
    from {{ref('stg_vendor')}} 
),

orders as(
    select * 
    from {{ref('stg_order')}}
),


dimension as(
    select DISTINCT
            CASE WHEN sv."vendor_id" IS NOT NULL THEN SV."vendor_id"
                WHEN so."vendor_id" IS NOT NULL THEN SO."vendor_id"
                ELSE NULL END AS "vendor_id",
            COALESCE (nullif(trim(sv."vendor_name"), ''), 'N/A') AS "vendor_name",
            CASE WHEN sv."product_id" IS NOT NULL THEN SV."product_id"
                WHEN so."product_id" IS NOT NULL THEN SO."product_id"
                ELSE NULL END AS "product_id",
            CASE WHEN sv."suit_number" IS NOT NULL THEN SV."suit_number"
                WHEN so."suit_number" IS NOT NULL THEN SO."suit_number"
                ELSE NULL END AS "suit_number",
            COALESCE (nullif(sv."quantity", 0)) AS "quantity",
            current_timestamp as current_date
           from vendor sv
           FULL OUTER JOIN orders so on so."vendor_id" = sv."vendor_id"
)


{{ surrogate_key('vendor_id','unique_vendor_id') }}