{{config(materialized='table')}}
 
 select *
 from {{ref('order')}}