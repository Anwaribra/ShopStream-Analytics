{{ config(materialized='table') }}

select
  o.order_id,
  o.customer_id,
  o.order_date,
  o.total_amount,
  o.status,
  c.email as customer_email
from {{ ref('orders') }} o
left join {{ ref('customers') }} c on c.customer_id = o.customer_id

