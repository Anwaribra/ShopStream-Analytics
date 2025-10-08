{{ config(materialized='table') }}

select
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.created_at,
  c.updated_at
from {{ ref('customers') }} c

