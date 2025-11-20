{{ config(
    materialized='table',
    unique_key=['anime_id', 'studio_id'] 
) }}

with unnested_studios as (
    SELECT
        cast({{ surrogate_key(["mal_id"]) }} as string) AS anime_id,      
        lower(trim(f.value::string)) as studio_name_raw 
        
    FROM {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.studios)) f
    
    WHERE a.studios IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

select
    us.anime_id,
    s.studio_id,
    {{ surrogate_key(['us.anime_id', 's.studio_id']) }} as anime_studio_key
from unnested_studios us
join {{ ref('stg_anime__studios') }} s
    on us.studio_name_raw = s.studio_name
where us.studio_name_raw is not null