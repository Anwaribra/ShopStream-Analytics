{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with source_orders as (
    select 
        record_value,
        event_ts,
        ingested_at
    from {{ source('bronze', 'raw_events') }}
    where topic = 'orders'
    {% if is_incremental() %}
    and event_ts > (select max(created_at) from {{ this }})
    {% endif %}
),

cleaned_orders as (
    select 
        record_value->>'order_id' as order_id,
        record_value->>'customer_id' as customer_id,
        coalesce((record_value->>'total_amount')::decimal,
                 (record_value->>'subtotal')::decimal) as total_amount,
        record_value->>'status' as status,
        coalesce((record_value->>'order_date')::timestamp, event_ts) as order_date,
        coalesce((record_value->>'created_at')::timestamp, event_ts) as created_at,
        coalesce((record_value->>'updated_at')::timestamp, ingested_at) as updated_at
    from source_orders
)

select distinct on (order_id)
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    created_at,
    updated_at
from cleaned_orders
order by order_id, updated_at desc

