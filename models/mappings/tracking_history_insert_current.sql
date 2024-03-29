{{
    config(
        materialized = 'table',
        tags = ['mappings'],
        unique_key = 'customer_id',
        pre_hook = {"sql" : "create sequence if not exists customer_id as int
                        start 1
                        increment 1;" 
                    }
    )
}}

with sq_customers  as(
    select
    "customer_id",
    "company",
    "first_name",
    "last_name",
    "address1",
    "address2",
    "city",
    "state",
    "postal_code",
    "phone",
    "email"
    from {{ref('stg_tracking_history')}}
),

{% if is_incremental() %}
lkp_customer_hist as(
    select
    s."customer_id"
    from sq_customers s
   -- current_timestamp as "refresh_dte"
    union
    select
    d."customer_id"
    from {{this}} d
),
{% endif %}

exp_anchor_action_flag as(
    select 
 --   l."cust_hist_seq_id" as "lk_cust_hist_seq_id",
 --   l."refresh_dte" as "lk_run_dte",
    s."customer_id",
    s."company",
    s."first_name",
    s."last_name",
    s."address1",
    s."address2",
    s."city",
    s."state",
    s."postal_code",
    s."phone",
    s."email",
    -- case 
    -- when "cust_hist_seq_id" is null then 1
    -- else to_integer("cust_hist_seq_id" + 1)
    -- end  as "cust_hist_seq_id",
    current_timestamp as "wrhse_start_dte_insert",
    'null' as "wrhse_end_dte_insert",
    current_date + interval '-1' as "wrhse_end_dte_update"
    from sq_customers s 
    {% if is_incremental() %}
    left join lkp_customer_hist l on l."customer_id" = s."customer_id"
    {% endif %}
),

rtr_insert_current as(
    select
    s."customer_id",
    s."company",
    s."first_name",
    s."last_name",
    s."address1",
    s."address2",
    s."city",
    s."state",
    s."postal_code",
    s."phone",
    s."email"
    from exp_anchor_action_flag s
),

tracking_history_insert_cur as (
    select
    s."customer_id",
    s."company",
    s."first_name",
    s."last_name",
    s."address1",
    s."address2",
    s."city",
    s."state",
    s."postal_code",
    s."phone",
    s."email"
    from rtr_insert_current s
)

select * from tracking_history_insert_cur