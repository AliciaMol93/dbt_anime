{{ config(materialized="table") }}

select
    r.anime_id,
    g.genre_id,
    g.genre_name
from {{ ref('stg_anime__genre_rel_anime') }} r
join {{ ref('stg_anime__genres') }} g
    on r.genre_id = g.genre_id
