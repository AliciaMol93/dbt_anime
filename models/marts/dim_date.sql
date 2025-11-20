{{ config(materialized='table', unique_key='date_sk') }}

with dates as (
  select generate_series::date as dt
  from generate_series('2000-01-01'::date, 
  current_date + interval '365 days', interval '1 day')
)

select
  row_number() over (order by dt) as date_sk,
  dt as date,
  extract(year from dt)::int as year,
  extract(month from dt)::int as month,
  extract(day from dt)::int as day,
  extract(quarter from dt)::int as quarter,
  case when extract(dow from dt) in (0,6) then 1 else 0 end as is_weekend,
  case
    when extract(month from dt) in (3,4,5) then 'spring'
    when extract(month from dt) in (6,7,8) then 'summer'
    when extract(month from dt) in (9,10,11) then 'fall'
    else 'winter'
  end as season_name,
  to_char(dt, 'YYYY-MM') as year_month
from dates
;
