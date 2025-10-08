{{ config(materialized='table') }}

select
  p.product_id,
  p.name,
  p.description,
  p.price,
  p.category,
  p.created_at,
  p.updated_at
from {{ ref('products') }} p

