{{ config(
    materialized='table',
    unique_key=['anime_id', 'streaming_id'] 
) }}

with unnested_streaming as (
    SELECT
        a.mal_id as anime_id,        
        lower(trim(f.value::string)) as streaming_name_raw 
        
    FROM {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.streaming)) f
    
    WHERE a.streaming IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

-- Resultado Final: Mapear Anime ID a streaming ID
SELECT
    t1.anime_id,
    
    -- Generar la clave foránea del género
    {{ surrogate_key(['t1.streaming_name_raw']) }} as streaming_id,
    
    -- Clave primaria compuesta para unicidad
    {{ surrogate_key(['t1.anime_id', 't1.streaming_name_raw']) }} as anime_streaming_key 

FROM unnested_streaming t1

-- Excluimos valores nulos para asegurar la integridad de la clave
WHERE t1.streaming_name_raw IS NOT NULL