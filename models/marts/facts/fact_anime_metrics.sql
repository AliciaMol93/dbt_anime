{{ config(
    materialized="table",
    unique_key="anime_id"
) }}

with anime_base as (
    select
        a.anime_id,
        a.type,
        a.status,
        a.source,
        cast(a.episodes as int) as episodes,
        a.season,
        cast(a.year as int) as year,
        a.rating as rating,
        a.url,
        a.image_url,
        at.title_clean as title,
        at.title_japanese as title_japanese
    from {{ ref('dim_anime_anime') }} a
    left join {{ ref('stg_anime__titles') }} at
        on a.anime_id = at.anime_id
),

votes_stats as (
    select
        anime_id,
        sum(try_to_number(score) * try_to_number(votes)) / nullif(sum(try_to_number(votes)),0) as avg_score,
        sum(try_to_number(votes)) as votes_count
    from ANIME_SILVER.dbt_amartinolmos.stg_anime__scores
    group by anime_id
),

engagement_latest as (
    select *
    from (
        select
            anime_id,
            try_to_number(total_users_interacting_count) as total_engagement,
            try_to_number(watching_count) as watching_count,
            try_to_number(completed_count) as completed_count,
            try_to_number(dropped_count) as dropped_count,
            try_to_number(on_hold_count) as on_hold_count,
            try_to_number(plan_to_watch_count) as plan_to_watch_count,
            cast(ingestion_ts as date) as stat_date,
            row_number() over (partition by anime_id order by ingestion_ts desc) as rn
        from {{ ref('stg_anime__stats') }}
    )
    where rn = 1
),

engagement_with_date as (
    select
        e.*,
        dd.date_id
    from engagement_latest e
    left join {{ ref('dim_anime_date') }} dd
        on e.stat_date = dd.date
)

select
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
    ab.url,
    ab.image_url,
    vs.avg_score,
    vs.votes_count,
    e.total_engagement,
    e.watching_count,
    e.completed_count,
    e.dropped_count,
    e.on_hold_count,
    e.plan_to_watch_count,
    case 
        when vs.avg_score is not null and e.total_engagement is not null
        then vs.avg_score * ln(1 + e.total_engagement)
        else null
    end as success_index,
    e.date_id,
    current_timestamp() as loaded_at
from anime_base ab
left join votes_stats vs on ab.anime_id = vs.anime_id
left join engagement_with_date e on ab.anime_id = e.anime_id
