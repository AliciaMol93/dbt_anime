{{ config(
    materialized="table"
) }}

with distinct_genres as (
    select distinct
        {{ surrogate_key(["genre_name_raw"]) }} as genre_id,
        genre_name_raw as genre_name
    from {{ ref('stg_anime__genres') }}
)

select *
from distinct_genres
