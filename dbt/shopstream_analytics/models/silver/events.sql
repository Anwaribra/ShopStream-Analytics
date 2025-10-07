{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

with source_events as (
    select 
        record_value,
        event_ts,
        ingested_at
    from {{ source('bronze', 'raw_events') }}
    where topic = 'events'
    {% if is_incremental() %}
    and event_ts > (select max(created_at) from {{ this }})
    {% endif %}
),

cleaned_events as (
    select 
        record_value->>'event_id' as event_id,
        record_value->>'user_id' as customer_id,
        record_value->>'event_type' as event_type,
        record_value as event_data,
        coalesce((record_value->>'created_at')::timestamp, event_ts) as created_at
    from source_events
)

select distinct on (event_id)
    event_id,
    customer_id,
    event_type,
    event_data,
    created_at
from cleaned_events
order by event_id, created_at desc

