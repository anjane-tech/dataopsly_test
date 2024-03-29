{%- macro surrogate_key(primary_key,unique_key) -%}
    select
    {{dbt_utils.surrogate_key(
       ([primary_key]) 
    )}} as {{ unique_key }}, *
    from dimension
{% endmacro %}

{%- macro fact_surrogate_key(primary_key,unique_key) -%}
    select
    {{dbt_utils.surrogate_key(
       ([primary_key]) 
    )}} as {{ unique_key }}, *
    from fact
{% endmacro %}



-- select
--   {{dbt_utils.surrogate_key(
--         ['"vendor_id"']
--   )}} as "unique_vendor_id", *
--   from dimension