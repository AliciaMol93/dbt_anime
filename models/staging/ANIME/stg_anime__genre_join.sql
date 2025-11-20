{{ config(materialized="table") }}

with
    unnested_genres as (
        select
            cast({{ surrogate_key(["mal_id"]) }} as string) as anime_id,
            lower(trim(f.value::string)) as genre_name_raw
        from
            {{ source("anime_source", "DETAILS") }} a,
            lateral flatten(input => parse_json(a.genres)) f
        where
            a.genres is not null
            and f.value::string is not null
            and f.value::string <> '[]'
    )

select
    g.anime_id,
    sg.genre_id,
    {{ surrogate_key(["g.anime_id", "sg.genre_id"]) }} as anime_genre_key
from unnested_genres g
join {{ ref('stg_anime__genres') }} sg
    on g.genre_name_raw = sg.genre_name
where g.genre_name_raw is not null
