{{ config(
    materialized='table',
    tags=['marts', 'dimension']
) }}

with date_range as (
    -- Genera un rango de fechas solo para 2025
    select
        dateadd(day, seq4(), '2025-01-01'::date) as date
    from table(generator(rowcount => 366)) -- 2025 es año normal
),

season_calc as (
    select
        date,
        extract(year from date) as year,
        extract(month from date) as month,
        extract(day from date) as day,
        case
            when month in (12,1,2) then 'Winter'
            when month in (3,4,5) then 'Spring'
            when month in (6,7,8) then 'Summer'
            when month in (9,10,11) then 'Fall'
        end as season
    from date_range
)

select
    row_number() over (order by date) as date_id,
    date,
    year,
    month,
    day,
    season
from season_calc
order by date
