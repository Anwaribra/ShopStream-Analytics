{{ config(materialized='table') }}

select
  oi.order_item_id,
  oi.order_id,
  oi.product_id,
  oi.quantity,
  oi.unit_price,
  (oi.quantity * oi.unit_price) as line_total,
  o.order_date,
  o.customer_id
from {{ ref('order_items') }} oi
left join {{ ref('orders') }} o on o.order_id = oi.order_id

