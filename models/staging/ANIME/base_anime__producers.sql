{{ config(materialized="table") }}

with unnested_producers as (
    select
        mal_id AS anime_id,
        f.value::string as producer_name_raw 
    from {{ source('anime_source', 'DETAILS') }} a,
         lateral flatten(input => parse_json(a.producers)) f
    where a.producers is not null
      and f.value::string is not null
      and f.value::string <> '[]'
)
    select
        anime_id,
        lower(trim(producer_name_raw)) as producer_name,
        {{ surrogate_key(["(producer_name_raw)"]) }} as producer_id
    from unnested_producers
    where producer_name is not null

