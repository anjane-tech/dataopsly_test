{{config(materialized='table')}}
 
 select * 
 from {{ref('seller')}}