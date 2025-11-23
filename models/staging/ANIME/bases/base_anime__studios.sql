{{ config(
    materialized='table'
) }}

with base_studios as (
    select
        mal_id,      
        f.value::string as studio_name_raw 
        
    from {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.studios)) f
    
    where a.studios IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

select
    {{ surrogate_key(["mal_id"]) }} as anime_id,
    lower(trim(studio_name_raw)) as studio_name,
    {{ surrogate_key(["lower(trim(studio_name_raw))"]) }} as studio_id
from base_studios
where lower(trim(studio_name_raw)) is not null