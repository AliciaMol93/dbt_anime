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
        listagg(g.genre_name, ', ') as genres
    from {{ ref('stg_anime__genre_join') }} j
    join {{ ref('stg_anime__genres') }} g
        on j.genre_id = g.genre_id
    group by j.anime_id
),

themes as (
    select
        j.anime_id,
        listagg(t.theme_name, ', ') as themes
    from {{ ref('stg_anime__theme_join') }} j
    join {{ ref('stg_anime__themes') }} t
        on j.theme_id = t.theme_id
    group by j.anime_id
),

studios as (
    select
        j.anime_id,
        listagg(s.studio_name, ', ') as studios
    from {{ ref('stg_anime__studio_join') }} j
    join {{ ref('stg_anime__studios') }} s
        on j.studio_id = s.studio_id
    group by j.anime_id
),

licensors as (
    select
        j.anime_id,
        listagg(l.licensor_name, ', ') as licensors
    from {{ ref('stg_anime__licensor_join') }} j
    join {{ ref('stg_anime__licensors') }} l
        on j.licensor_id = l.licensor_id
    group by j.anime_id
),

streaming as (
    select
        j.anime_id,
        listagg(s.streaming_name, ', ') as streaming_platforms
    from {{ ref('stg_anime__streaming_join') }} j
    join {{ ref('stg_anime__streaming') }} s
        on j.streaming_id = s.streaming_id
    group by j.anime_id
)

select
    b.*,
    g.genres,
    t.themes,
    s.studios,
    l.licensors,
    st.streaming_platforms
from base b
left join genres g on b.anime_id = g.anime_id
left join themes t on b.anime_id = t.anime_id
left join studios s on b.anime_id = s.anime_id
left join licensors l on b.anime_id = l.anime_id
left join streaming st on b.anime_id = st.anime_id
