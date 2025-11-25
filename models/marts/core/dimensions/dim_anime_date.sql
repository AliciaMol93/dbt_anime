{{ config(
    materialized='table',
    tags=['marts','dimension']
) }}

with
-- bounds: min/max ingestion_ts from stats and titles (useful sources)
min_max as (
    select
        least(
            coalesce((select min(cast(ingestion_ts as date)) from {{ ref('stg_anime__stats') }}), NULL),
            coalesce((select min(cast(ingestion_ts as date)) from {{ ref('stg_anime__titles') }}), NULL)
        ) as min_dt_candidate,
        greatest(
            coalesce((select max(cast(ingestion_ts as date)) from {{ ref('stg_anime__stats') }}), current_date()),
            coalesce((select max(cast(ingestion_ts as date)) from {{ ref('stg_anime__titles') }}), current_date())
        ) as max_dt_candidate
),

bounds as (
    select
        coalesce(min_dt_candidate, to_date('1917-01-01')) as min_dt,
        greatest(coalesce(max_dt_candidate, current_date()), dateadd(day, 365, current_date())) as max_dt
    from min_max
),

-- recursive date generation from min_dt to max_dt
date_seq as (
    select min_dt as dt, max_dt from bounds
    union all
    select dateadd(day, 1, dt), max_dt
    from date_seq
    where dt < max_dt
),

season_calc as (
    select
        row_number() over (order by dt) as date_id,
        dt as date,
        extract(year from dt) as year,
        extract(month from dt) as month,
        extract(day from dt) as day,
        case
            when extract(month from dt) in (12,1,2) then 'Winter'
            when extract(month from dt) in (3,4,5) then 'Spring'
            when extract(month from dt) in (6,7,8) then 'Summer'
            else 'Fall'
        end as season
    from date_seq
)

select
    date_id,
    date,
    year,
    month,
    day,
    season
from season_calc
order by date
