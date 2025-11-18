{{ config(
    materialized = "table"
) }}

with unnested_producers as (
    select
        a.mal_id as anime_id,
        lower(trim(f.value::string)) as producer_name_raw 
    from {{ source('anime_source', 'DETAILS') }} a,
         lateral flatten(input => parse_json(a.producers)) f
    where a.producers is not null
      and f.value::string is not null
      and f.value::string <> '[]'
)

select
    t1.anime_id,
    {{ surrogate_key(['t1.producer_name_raw']) }} as producer_id,
    {{ surrogate_key(['t1.anime_id', 't1.producer_name_raw']) }} as anime_producer_key

from unnested_producers t1
where t1.producer_name_raw is not null
