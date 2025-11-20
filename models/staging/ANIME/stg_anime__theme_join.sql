{{ config(
    materialized='table',
    unique_key=['anime_id', 'theme_id'] 
) }}

with unnested_themes as (
    SELECT
        cast({{ surrogate_key(["mal_id"]) }} as string) AS anime_id,       
        lower(trim(f.value::string)) as theme_name_raw 
        
    FROM {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.themes)) f
    
    WHERE a.themes IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

select
    ut.anime_id,
    t.theme_id,
    {{ surrogate_key(['ut.anime_id', 't.theme_id']) }} as anime_theme_key
from unnested_themes ut
join {{ ref('stg_anime__themes') }} t
    on ut.theme_name_raw = t.theme_name
where ut.theme_name_raw is not null