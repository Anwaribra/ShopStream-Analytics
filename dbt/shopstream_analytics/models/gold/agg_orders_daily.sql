{{ config(materialized='table') }}

with base as (
  select 
    date_trunc('day', order_date)::date as order_date,
    count(*) as orders,
    sum(total_amount) as revenue
  from {{ ref('orders') }}
  group by 1
)
select * from base
order by order_date desc

