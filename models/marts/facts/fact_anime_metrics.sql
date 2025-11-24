{{ config(
    materialized="table",
    unique_key="anime_id",
    on_schema_change="sync_all_columns"
) }}

with anime_base as (
    select
        a.anime_id,
        a.type,
        a.status,
        a.source,
        a.episodes,
        a.season,
        a.year,
        a.rating,
        a.title,
        a.title_japanese
    from {{ ref('dim_anime_anime') }} a
    
),

votes_stats as (
    select
        anime_id,
        sum(score * votes) / nullif(sum(votes), 0) as avg_score,
        sum(votes) as votes_count,
        stddev_pop(score) as score_dispersion
    from {{ ref('stg_anime__scores') }}
    group by anime_id
),

engagement as (
    select
        anime_id,
        total_users_interacting_count as total_engagement,
        watching_count,
        completed_count,
        dropped_count,
        on_hold_count,
        plan_to_watch_count,
    from {{ ref('stg_anime__stats') }}
)

select distinct
    ab.anime_id,
    ab.title,
    ab.title_japanese,
    ab.type,
    ab.status,
    ab.source,
    ab.episodes,
    ab.season,
    ab.year,
    ab.rating,

    vs.avg_score,
    vs.votes_count,
    vs.score_dispersion,

    e.total_engagement,
    e.watching_count,
    e.completed_count,
    e.dropped_count,
    e.on_hold_count,
    e.plan_to_watch_count,
    (coalesce(vs.avg_score, 0) * ln(1 + coalesce(e.total_engagement, 0))) as success_index,

    current_timestamp() as loaded_at
from anime_base ab
left join votes_stats vs on ab.anime_id = vs.anime_id
left join engagement e on ab.anime_id = e.anime_id
