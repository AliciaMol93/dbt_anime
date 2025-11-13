{{ config(
    materialized='table',
    unique_key=['anime_id', 'theme_id'] 
) }}

with unnested_themes as (
    SELECT
        anime.anime_id,        
        -- 2. VALOR BRUTO: El nombre del género desanidado
        lower(trim(f.value::string)) as theme_name_raw 
        
    FROM {{ ref('stg_anime__details') }} anime,
    lateral flatten(input => PARSE_JSON(anime.themes)) f
    
    WHERE anime.themes IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

-- Resultado Final: Mapear Anime ID a studio ID
SELECT
    t1.anime_id,
    
    -- Generar la clave foránea del género
    {{ dbt_utils.generate_surrogate_key(['t1.theme_name_raw']) }} as theme_id,
    
    -- Clave primaria compuesta para unicidad
    {{ dbt_utils.generate_surrogate_key(['t1.anime_id', 't1.theme_name_raw']) }} as anime_theme_key 

FROM unnested_themes t1

-- Excluimos valores nulos para asegurar la integridad de la clave
WHERE t1.theme_name_raw IS NOT NULL