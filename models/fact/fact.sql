{{
      config(
      materialized = 'incremental',
      tags = ["fact"],
      unique_key = "'unique_order_id'"
    )
}}

with fact as(
    select 
      MD5(cus."unique_cust_id" || prod."unique_prod_id" || sel."unique_seller_id" || ven."unique_vendor_id" || ord."order_id") AS md5_hash,
      cus."unique_cust_id",
      prod."unique_prod_id",
      sel."unique_seller_id",
      ven."unique_vendor_id", 
      ord."order_id",
      sum(prod."price") as "price",
      sum(ven."quantity") as "quantity"
      from {{ref("stg_order")}} ord
      INNER JOIN {{ref("dim_customer")}} cus on cus."customer_id" = coalesce(ord."customer_id",NULL) 
      INNER JOIN {{ref("dim_products")}} prod on prod."product_id" = coalesce(ord."product_id",NULL) 
      INNER JOIN {{ref("dim_seller")}} sel on sel."suit_number" = coalesce(ord."suit_number",NULL)  
      INNER JOIN {{ref("dim_vendor")}} ven on ven."vendor_id" = coalesce(ord."vendor_id",NULL)
      group by
      cus."unique_cust_id",
      prod."unique_prod_id",
      sel."unique_seller_id",
      ven."unique_vendor_id",
      ord."order_id"
)

{{ fact_surrogate_key('order_id','unique_order_id') }}  