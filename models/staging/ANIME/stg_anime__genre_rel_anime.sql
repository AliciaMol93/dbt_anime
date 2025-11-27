{{ config(materialized="table") }}

select distinct
    anime_id, 
    genre_id, 
    {{ surrogate_key(['anime_id', 'genre_id']) }} as anime_genre_key
from {{ ref("base_anime__genres") }}
where genre_name is not null
