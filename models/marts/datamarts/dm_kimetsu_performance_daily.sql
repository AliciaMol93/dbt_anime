{{ config(
    materialized = "table"
) }}

WITH k AS (
    SELECT
        anime_id,
        version_name,
        canonical_group,
        title,
        type,
        season,
        release_year
    FROM {{ ref('dim_kimetsu_version') }}
),

f AS (
    SELECT
        anime_id,
        date_id,
        stat_date,
        avg_score,
        success_index,
        total_engagement,
        watching_count,
        completed_count,
        dropped_count,
        on_hold_count,
        plan_to_watch_count,
        completion_ratio,
        drop_rate
    FROM {{ ref('fact_anime_performance_daily') }}
)

SELECT
    f.anime_id,
    k.version_name,
    k.canonical_group,
    k.title,
    k.type,
    k.release_year,
    k.season,

    f.stat_date,
    f.date_id,
    f.avg_score,
    f.success_index,
    f.total_engagement,
    f.watching_count,
    f.completed_count,
    f.dropped_count,
    f.on_hold_count,
    f.plan_to_watch_count,
    f.completion_ratio,
    f.drop_rate,


FROM f
JOIN k USING (anime_id)


