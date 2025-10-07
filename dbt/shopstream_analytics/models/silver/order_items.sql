{{ config(
    materialized='incremental',
    unique_key='order_item_id'
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

order_items as (
    select 
        so.record_value->>'order_id' as order_id,
        p.product,
        p.product_index,
        so.event_ts
    from source_orders so
    cross join lateral jsonb_array_elements(so.record_value->'items') with ordinality as p(product, product_index)
),

cleaned_order_items as (
    select 
        order_id,
        product->>'id' as product_id,
        (product->>'quantity')::integer as quantity,
        (product->>'price')::decimal as unit_price,
        -- Stable, collision-safe key using order_id text + ordinal
        md5(order_id || '-' || product_index::text) as order_item_id,
        event_ts as created_at
    from order_items
)

select distinct on (order_item_id)
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at
from cleaned_order_items
order by order_item_id, created_at desc

