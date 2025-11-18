{{ config(materialized="table", unique_key=["anime_id", "genre_id"]) }}

with
    unnested_genres as (
        select anime.mal_id as anime_id, 
        lower(trim(f.value::string)) as genre_name_raw
        from
            {{ source("anime_source", "DETAILS") }} anime,
            lateral flatten(input => parse_json(anime.genres)) f
        where
            anime.genres is not null
            and f.value::string is not null
            and f.value::string <> '[]'
    )

select
    t1.anime_id,
    {{ surrogate_key(["t1.genre_name_raw"]) }} as genre_id,
    {{ surrogate_key(["t1.anime_id", "t1.genre_name_raw"]) }} as anime_genre_key
from unnested_genres t1
where t1.genre_name_raw is not null
