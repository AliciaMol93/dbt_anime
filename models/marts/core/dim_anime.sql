{{ config(
    materialized='table',
    unique_key='anime_id'
) }}

with base as (
    select
        d.anime_id,
        d.title,
        d.title_japanese,
        d.type,
        d.status,
        d.rank,
        d.score,
        d.scored_by,
        d.popularity,
        d.members,
        d.favorites,
        d.episodes,
        d.year,
        d.season,
        d.source,
        d.rating,
        d.demographics,
        d.url,
        d.image_url,
        d.synopsis,
        d.start_date,
        d.end_date,
        d.ingestion_ts
    from {{ ref('stg_anime__details') }} d
),

genres as (
    select
        j.anime_id,
        listagg(g.genre_name, ', ') as genres,
        count(*) as n_genres
    from {{ ref('stg_anime__genre_join') }} j
    join {{ ref('stg_anime__genres') }} g
        on j.genre_id = g.genre_id
    group by j.anime_id
),

themes as (
    select
        j.anime_id,
        listagg(t.theme_name, ', ') as themes,
        count(*) as n_themes
    from {{ ref('stg_anime__theme_join') }} j
    join {{ ref('stg_anime__themes') }} t
        on j.theme_id = t.theme_id
    group by j.anime_id
),

studios as (
    select
        j.anime_id,
        listagg(s.studio_name, ', ') as studios,
        count(*) as n_studios
    from {{ ref('stg_anime__studio_join') }} j
    join {{ ref('stg_anime__studios') }} s
        on j.studio_id = s.studio_id
    group by j.anime_id
),

streaming as (
    select
        j.anime_id,
        listagg(s.streaming_name, ', ') as streaming_platforms,
        count(*) as n_streaming_platforms
    from {{ ref('stg_anime__streaming_join') }} j
    join {{ ref('stg_anime__streaming') }} s
        on j.streaming_id = s.streaming_id
    group by j.anime_id
)

select
    b.*,
    g.genres,
    n.genres,
    t.themes,
    n_themes,
    s.studios,
    n_studios
    st.streaming_platforms,
    n_streaming_platforms
    ,

    case
         when b.score >= 8 then 'high'
         when b.score <= 6.5 then 'medium'
         else 'low'
    end as score_category,

    case 
        when b.rank >= 100 then 1
        when b.score >= 8 then 1
        else 0
    end as hit_flag,    

from base b
left join genres g on b.anime_id = g.anime_id
left join themes t on b.anime_id = t.anime_id
left join studios s on b.anime_id = s.anime_id
left join licensors l on b.anime_id = l.anime_id
left join streaming st on b.anime_id = st.anime_id
