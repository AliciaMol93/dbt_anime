{{ config(
    materialized="incremental",
    unique_key=["anime_id", "date_id"],
    incremental_strategy="merge",
    on_schema_change="sync_all_columns"
) }}

with

-- Scores históricos por anime
scores as (
    select
        anime_id,
        avg(score) as avg_score,           
        sum(votes) as total_votes,         
        stddev_pop(score) as score_dispersion
    from {{ ref('stg_anime__scores') }}
    group by anime_id
),

-- Engagement diario
daily_stats as (
    select
        anime_id,
        total_users_interacting_count as total_engagement,
        watching_count,
        completed_count,
        dropped_count,
        on_hold_count,
        plan_to_watch_count,
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

    -- success index diario
    (coalesce(s.avg_score, 0) * ln(1 + coalesce(d.total_engagement, 0))) as success_index

from daily_with_date d
left join scores s on d.anime_id = s.anime_id

{% if is_incremental() %}
    where d.date_id > (select coalesce(max(date_id), 0) from {{ this }})
{% endif %}
