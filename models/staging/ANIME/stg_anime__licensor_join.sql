{{ config(materialized="table") }}

with unnested_licensors as (
    select
        cast({{ surrogate_key(["mal_id"]) }} as string) AS anime_id,
        lower(trim(f.value::string)) as licensor_name_raw
    from {{ source("anime_source", "DETAILS") }} a,
         lateral flatten(input => parse_json(a.licensors)) f
    where
        a.licensors is not null
        and f.value::string is not null
        and f.value::string <> '[]'
)

select
    ul.anime_id,
    l.licensor_id,
    {{ surrogate_key(["ul.anime_id", "l.licensor_id"]) }} as anime_licensor_key
from unnested_licensors ul
join {{ ref('stg_anime__licensors') }} l
    on ul.licensor_name_raw = l.licensor_name
where ul.licensor_name_raw is not null
