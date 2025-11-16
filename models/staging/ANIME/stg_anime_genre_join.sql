{{ config(
    materialized='table',
    unique_key=['anime_id', 'genre_id'] 
) }}

with unnested_genres as (
    SELECT
        anime.anime_id,        
        -- 2. VALOR BRUTO: El nombre del género desanidado
        lower(trim(f.value::string)) as genre_name_raw 
        
    FROM {{ ref('stg_anime__details') }} anime,
    lateral flatten(input => PARSE_JSON(anime.genres)) f
    
    WHERE anime.genres IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

-- Resultado Final: Mapear Anime ID a Genre ID
SELECT
    t1.anime_id,
    
    -- Generar la clave foránea del género
    {{ surrogate_key(['t1.genre_name_raw']) }} as genre_id,
    
    -- Clave primaria compuesta para unicidad
    {{ surrogate_key(['t1.anime_id', 't1.genre_name_raw']) }} as anime_genre_key 

FROM unnested_genres t1

-- Excluimos valores nulos para asegurar la integridad de la clave
WHERE t1.genre_name_raw IS NOT NULL