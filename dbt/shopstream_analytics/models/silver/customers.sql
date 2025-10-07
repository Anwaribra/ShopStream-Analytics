{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with source_customers as (
    select 
        record_value,
        event_ts,
        ingested_at
    from {{ source('bronze', 'raw_events') }}
    where topic = 'customers'
    {% if is_incremental() %}
    and event_ts > (select max(created_at) from {{ this }})
    {% endif %}
),

cleaned_customers as (
    select 
        record_value->>'customer_id' as customer_id,
        record_value->>'first_name' as first_name,
        record_value->>'last_name' as last_name,
        record_value->>'email' as email,
        coalesce((record_value->>'created_at')::timestamp, event_ts) as created_at,
        coalesce((record_value->>'updated_at')::timestamp, ingested_at) as updated_at
    from source_customers
)

select distinct on (customer_id)
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at
from cleaned_customers
order by customer_id, updated_at desc

