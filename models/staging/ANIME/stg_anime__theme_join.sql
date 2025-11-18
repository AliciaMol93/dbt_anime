{{ config(
    materialized='table',
    unique_key=['anime_id', 'theme_id'] 
) }}

with unnested_themes as (
    SELECT
        a.mal_id as anime_id,        
        lower(trim(f.value::string)) as theme_name_raw 
        
    FROM {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.themes)) f
    
    WHERE a.themes IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

SELECT
    t1.anime_id,
    
    -- Generar la clave foránea del género
    {{ dbt_utils.generate_surrogate_key(['t1.theme_name_raw']) }} as theme_id,
    
    -- Clave primaria compuesta para unicidad
    {{ dbt_utils.generate_surrogate_key(['t1.anime_id', 't1.theme_name_raw']) }} as anime_theme_key 

FROM unnested_themes t1

-- Excluimos valores nulos para asegurar la integridad de la clave
WHERE t1.theme_name_raw IS NOT NULL