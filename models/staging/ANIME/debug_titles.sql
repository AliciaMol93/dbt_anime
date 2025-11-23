{{ config(materialized="table") }}

-- Modelo de debugging SIN incremental
with source_data as (
    select
        mal_id,
        title,
        title_japanese,
        -- Probar la surrogate_key
        {{ surrogate_key(["mal_id"]) }} as anime_id_calculado,
        -- Probar la limpieza de títulos
        case
            when trim(title) like '% Season %' 
                then trim(regexp_replace(trim(title), ' Season [0-9]+', ''))
            when trim(title) like '% Part %' 
                then trim(regexp_replace(trim(title), ' Part [0-9]+', ''))
            else trim(title)
        end as title_clean
    from {{ source("anime_source", "DETAILS") }}
)
select *
from source_data
where title like '%fullmetal%' or title like '%attack%'
limit 20