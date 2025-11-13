{{ config(
    materialized='table',
    unique_key=['anime_id', 'streaming_id'] 
) }}

with unnested_streaming as (
    SELECT
        anime.anime_id,        
        -- 2. VALOR BRUTO: El nombre del género desanidado
        lower(trim(f.value::string)) as streaming_name_raw 
        
    FROM {{ ref('stg_anime__details') }} anime,
    lateral flatten(input => PARSE_JSON(anime.streaming)) f
    
    WHERE anime.streaming IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

-- Resultado Final: Mapear Anime ID a streaming ID
SELECT
    t1.anime_id,
    
    -- Generar la clave foránea del género
    {{ dbt_utils.generate_surrogate_key(['t1.streaming_name_raw']) }} as streaming_id,
    
    -- Clave primaria compuesta para unicidad
    {{ dbt_utils.generate_surrogate_key(['t1.anime_id', 't1.streaming_name_raw']) }} as anime_streaming_key 

FROM unnested_streaming t1

-- Excluimos valores nulos para asegurar la integridad de la clave
WHERE t1.streaming_name_raw IS NOT NULL