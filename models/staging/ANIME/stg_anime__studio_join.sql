{{ config(
    materialized='table',
    unique_key=['anime_id', 'studio_id'] 
) }}

with unnested_studios as (
    SELECT
        a.mal_id as anime_id,        
        lower(trim(f.value::string)) as studio_name_raw 
        
    FROM {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.studios)) f
    
    WHERE a.studios IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

-- Resultado Final: Mapear Anime ID a studio ID
SELECT
    t1.anime_id,
    
    -- Generar la clave foránea del género
    {{surrogate_key(['t1.studio_name_raw']) }} as studio_id,
    
    -- Clave primaria compuesta para unicidad
    {{ surrogate_key(['t1.anime_id', 't1.studio_name_raw']) }} as anime_studio_key 

FROM unnested_studios t1

WHERE t1.studio_name_raw IS NOT NULL