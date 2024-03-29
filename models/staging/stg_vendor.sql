{{config(materialized='table')}}
 
 select * 
 from {{ref('vendor')}}