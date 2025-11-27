{{ config(materialized="table") }}

with raw as (
    select 
        a.mal_id,
        lower(trim(f.value::string)) as producer_name
    from {{ source('anime_source', 'DETAILS') }} a,
         lateral flatten(input => parse_json(a.producers)) f
    where a.producers is not null
),

clean as (
    select distinct
        {{ surrogate_key(['mal_id']) }} as anime_id,
        producer_name
    from raw
    where producer_name is not null
      and producer_name <> ''
)

select
    anime_id,
    producer_name,
    {{ surrogate_key(['producer_name']) }} as producer_id
from clean
order by anime_id, producer_name
