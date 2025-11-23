{{ config(materialized="table") }}

with
    base_producers as (

        select 
            mal_id, 
            f.value::string as producer_name_raw
        from
            {{ source("anime_source", "DETAILS") }} a,
            lateral flatten(input => parse_json(a.producers)) f
        where
            a.producers is not null
            and f.value::string is not null
            and f.value::string <> '[]'
    )
select
    {{ surrogate_key(["mal_id"]) }} as anime_id,
    lower(trim(producer_name_raw)) as producer_name,
    {{ surrogate_key(["lower(trim(producer_name_raw))"]) }} as producer_id
from base_producers
where lower(trim(producer_name_raw)) is not null
