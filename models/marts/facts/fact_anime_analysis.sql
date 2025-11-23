{{ config(
    materialized="incremental",
    unique_key=["anime_id", "date_id"]
) }}

with

anime_base as (
    select *
    from {{ ref('dim_anime_anime') }}
),

anime_genres as (
    select *
    from {{ ref('dim_anime_genres') }}
),

anime_studios as (
    select *
    from {{ ref('dim_anime_studios') }}
),

anime_producers as (
    select *
    from {{ ref('dim_anime_producers') }}
),

anime_directors as (
    select *
    from {{ ref('dim_anime_directors') }}
),

anime_themes as (
    select *
    from {{ ref('dim_anime_themes') }}
),

-- 7️⃣ Métricas históricas
metrics as (
    select *
    from {{ ref('fact_anime_metrics') }}
),

-- 8️⃣ Engagement diario
daily_stats as (
    select
        anime_id,
        cast(ingestion_ts as date) as stat_date,
        try_to_number(total_users_interacting_count) as total_engagement,
        try_to_number(watching_count) as watching_count,
        try_to_number(completed_count) as completed_count,
        try_to_number(dropped_count) as dropped_count,
        try_to_number(on_hold_count) as on_hold_count,
        try_to_number(plan_to_watch_count) as plan_to_watch_count
    from {{ ref('stg_anime__stats') }}
),

-- 9️⃣ Unimos con dim_date
daily_with_date as (
    select
        d.*,
        dd.date_id
    from daily_stats d
    left join {{ ref('dim_anime_date') }} dd
        on d.stat_date = dd.date
)

-- 10️⃣ Fact table final
select
    a.anime_id,
    d.date_id,
    d.stat_date,

    -- Dimensiones desnormalizadas
    g.genre_id,
    g.genre_name,
    s.studio_id,
    s.studio_name,
    p.producer_id,
    p.producer_name,
    dir.person_id as director_id,
    dir.director_name,
    th.theme_id,
    th.theme_name,

    -- Atributos base
    a.title,
    a.type,
    a.status,
    a.source,
    a.episodes,
    a.season,
    a.year,
    a.rating,
    a.url,
    a.image_url,

    -- Métricas
    m.avg_score,
    m.votes_count,
    m.score_dispersion,
    d.total_engagement,
    d.watching_count,
    d.completed_count,
    d.dropped_count,
    d.on_hold_count,
    d.plan_to_watch_count,

    -- Métrica compuesta: éxito ponderado por engagement diario
    (m.avg_score * ln(1 + d.total_engagement)) as success_index

from anime_base a
left join anime_genres g on a.anime_id = g.anime_id
left join anime_studios s on a.anime_id = s.anime_id
left join anime_producers p on a.anime_id = p.anime_id
left join anime_directors dir on a.anime_id = dir.anime_id
left join anime_themes th on a.anime_id = th.anime_id
left join daily_with_date d on a.anime_id = d.anime_id
left join metrics m on a.anime_id = m.anime_id

{% if is_incremental() %}
    where d.stat_date > (select max(stat_date) from {{ this }})
{% endif %}
