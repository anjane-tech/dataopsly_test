{{config(materialized='table')}}


select * from {{source('source_sf','sample_table')}}