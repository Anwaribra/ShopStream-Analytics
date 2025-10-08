{{ config(materialized='table') }}

with base as (
  select 
    date_trunc('day', created_at)::date as event_date,
    event_type,
    count(*) as events
  from {{ ref('events') }}
  group by 1,2
)
select * from base
order by event_date desc, event_type

