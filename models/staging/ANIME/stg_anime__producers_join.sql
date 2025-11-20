{{ config(
    materialized = "table"
) }}

with unnested_producers as (
    select
        cast({{ surrogate_key(["mal_id"]) }} as string) AS anime_id,
        lower(trim(f.value::string)) as producer_name_raw 
    from {{ source('anime_source', 'DETAILS') }} a,
         lateral flatten(input => parse_json(a.producers)) f
    where a.producers is not null
      and f.value::string is not null
      and f.value::string <> '[]'
)

select
    up.anime_id,
    p.producer_id,
    {{ surrogate_key(['up.anime_id', 'p.producer_id']) }} as anime_producer_key
from unnested_producers up
join {{ ref('stg_anime__producers') }} p
    on up.producer_name_raw = p.producer_name
where up.producer_name_raw is not null
