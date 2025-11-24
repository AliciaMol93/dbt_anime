{{ config(
    materialized="incremental",
    unique_key=["anime_id", "date_id", "genre_id", "studio_id", "producer_id", "director_id", "theme_id"],
    incremental_strategy="merge",
    on_schema_change="sync_all_columns"
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
    select anime_id, person_id as director_id, director_name
    from {{ ref('dim_anime_directors') }}
),

anime_themes as (
    select *
    from {{ ref('dim_anime_themes') }}
),

-- 7️⃣ Métricas globales
metrics as (
     select
        anime_id,
        avg_score,
        votes_count,
        score_dispersion
    from {{ ref('fact_anime_metrics') }}
),

-- 8️⃣ Engagement diario
daily as (
    select
        anime_id,
        date_id,
        stat_date,
        total_engagement,
        watching_count,
        completed_count,
        dropped_count,
        on_hold_count,
        plan_to_watch_count
    from {{ ref('fact_anime_daily') }}
)

-- 10️⃣ Fact table final
select distinct
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
    dir.director_id,
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
    coalesce(m.avg_score,0) * ln(1 + coalesce(d.total_engagement,0)) as success_index

from anime_base a
left join anime_genres g on a.anime_id = g.anime_id
left join anime_studios s on a.anime_id = s.anime_id
left join anime_producers p on a.anime_id = p.anime_id
left join anime_directors dir on a.anime_id = dir.anime_id
left join anime_themes th on a.anime_id = th.anime_id
left join daily d on a.anime_id = d.anime_id
left join metrics m on a.anime_id = m.anime_id

{% if is_incremental() %}
where d.date_id > (select coalesce(max(date_id),0) from {{ this }})
{% endif %}
