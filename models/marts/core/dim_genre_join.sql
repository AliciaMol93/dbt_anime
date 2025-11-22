{{ config(
    materialized="table"
) }}

select
    g.anime_id,
    dg.genre_id,
    {{ surrogate_key(["g.anime_id", "dg.genre_id"]) }} as anime_genre_key
from {{ ref('stg_anime__genre_rel_anime') }} g
join {{ ref('dim_genre') }} dg
    on g.genre_id = dg.genre_id
