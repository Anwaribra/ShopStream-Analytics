{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

with source_products as (
    select 
        record_value,
        event_ts,
        ingested_at
    from {{ source('bronze', 'raw_events') }}
    where topic = 'products'
    {% if is_incremental() %}
    and event_ts > (select max(created_at) from {{ this }})
    {% endif %}
),

cleaned_products as (
    select 
        record_value->>'product_id' as product_id,
        record_value->>'title' as name,
        record_value->>'description' as description,
        (record_value->>'price')::decimal as price,
        record_value->>'category' as category,
        coalesce((record_value->>'created_at')::timestamp, event_ts) as created_at,
        coalesce((record_value->>'updated_at')::timestamp, ingested_at) as updated_at
    from source_products
)

select distinct on (product_id)
    product_id,
    name,
    description,
    price,
    category,
    created_at,
    updated_at
from cleaned_products
order by product_id, updated_at desc

