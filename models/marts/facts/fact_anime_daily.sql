{{ config(
    materialized="incremental",
    unique_key=["anime_id", "date_id"]
) }}

with

-- Scores históricos por anime
scores as (
    select
        anime_id,
        avg(try_to_number(score)) as avg_score,           
        sum(try_to_number(votes)) as total_votes,         
        stddev_pop(try_to_number(score)) as score_dispersion
    from ANIME_SILVER.dbt_amartinolmos.stg_anime__scores
    group by anime_id
),

-- Engagement diario
daily_stats as (
    select
        anime_id,
        try_to_number(total_users_interacting_count) as total_engagement,
        try_to_number(watching_count) as watching_count,
        try_to_number(completed_count) as completed_count,
        try_to_number(dropped_count) as dropped_count,
        try_to_number(on_hold_count) as on_hold_count,
        try_to_number(plan_to_watch_count) as plan_to_watch_count,
        cast(ingestion_ts as date) as stat_date
    from {{ ref('stg_anime__stats') }}
),

-- Join con dim_date
daily_with_date as (
    select
        d.*,
        dd.date_id
    from daily_stats d
    left join {{ ref('dim_anime_date') }} dd
        on d.stat_date = dd.date
)

select
    d.anime_id,
    d.date_id,
    d.stat_date,
    s.avg_score,
    s.total_votes,
    s.score_dispersion,
    d.total_engagement,
    d.watching_count,
    d.completed_count,
    d.dropped_count,
    d.on_hold_count,
    d.plan_to_watch_count,
    (s.avg_score * ln(1 + d.total_engagement)) as success_index
from daily_with_date d
left join scores s on d.anime_id = s.anime_id

{% if is_incremental() %}
    where d.stat_date > (select max(stat_date) from {{ this }})
{% endif %}
