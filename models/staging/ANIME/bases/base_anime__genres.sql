{{ config(materialized="table") }}

with base_genres as (
    select
        mal_id,
        f.value::string as genre_name_raw

    from {{ source("anime_source", "DETAILS") }} a,
         lateral flatten(input => parse_json(a.genres)) f

    where a.genres is not null
      and f.value::string is not null
      and f.value::string <> '[]'
)

select
    {{ surrogate_key(["mal_id"]) }} as anime_id,
    lower(trim(genre_name_raw)) as genre_name,
    {{ surrogate_key(["lower(trim(genre_name_raw))"]) }} as genre_id
from base_genres
where lower(trim(genre_name_raw)) is not null
