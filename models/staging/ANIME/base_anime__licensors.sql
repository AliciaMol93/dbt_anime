{{ config(materialized="table") }}


with
    base_licensors as (
        select 
            mal_id,
            (f.value::string) as licensor_name_raw
        from
            {{ source("anime_source", "DETAILS") }} a,
            lateral flatten(input => parse_json(a.licensors)) f
        where
            a.licensors is not null
            and f.value::string <> '[]'
            and f.value::string is not null
    )
select
    {{ surrogate_key(["mal_id"]) }} as anime_id,
    lower(trim(licensor_name_raw)) as licensor_name,
    {{ surrogate_key(["licensor_name_raw"]) }} as licensor_id
from base_licensors
where licensor_name is not null
