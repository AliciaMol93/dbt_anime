{{ config(
    materialized='table',
    unique_key=['anime_id', 'licensor_id'] 
) }}

with unnested_licensors as (
    SELECT
        anime.anime_id,        
        -- 2. VALOR BRUTO: El nombre del género desanidado
        lower(trim(f.value::string)) as licensor_name_raw 
        
    FROM {{ ref('stg_anime__details') }} anime,
    lateral flatten(input => PARSE_JSON(anime.licensors)) f
    
    WHERE anime.licensors IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

-- Resultado Final: Mapear Anime ID a studio ID
SELECT
    t1.anime_id,
    
    -- Generar la clave foránea de la licencia
    {{ dbt_utils.generate_surrogate_key(['t1.licensor_name_raw']) }} as licensor_id,
    
    -- Clave primaria compuesta para unicidad
    {{ dbt_utils.generate_surrogate_key(['t1.anime_id', 't1.licensor_name_raw']) }} as anime_licensor_key 

FROM unnested_licensors t1

-- Excluimos valores nulos para asegurar la integridad de la clave
WHERE t1.licensor_name_raw IS NOT NULL