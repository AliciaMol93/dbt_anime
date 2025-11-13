{{ config(
    materialized='table', 
    unique_key='person_id'
) }}

WITH src_links AS (
    SELECT
        -- 1. CLAVE NATURAL
        PERSON_MAL_ID AS person_mal_id,
        
        -- 2. LIMPIEZA DE ENLACES
        NULLIF(LOWER(TRIM(URL)), '') AS mal_url,
        NULLIF(LOWER(TRIM(IMAGE_URL)), '') AS image_url,
        NULLIF(LOWER(TRIM(WEBSITE_URL)), '') AS website_url

    -- Leemos DIRECTAMENTE DE LA FUENTE (Bronze)
    FROM {{ source('anime_source', 'PERSON_DETAILS') }} 
)

SELECT
    -- Generamos la CLAVE SUBROGADA usando la clave natural
    {{ dbt_utils.generate_surrogate_key(["person_mal_id"]) }} AS person_id,
    
    mal_url,
    image_url,
    website_url

FROM src_links
WHERE person_mal_id IS NOT NULL -- Aseguramos que solo procesamos IDs válidos
  AND (mal_url IS NOT NULL OR image_url IS NOT NULL OR website_url IS NOT NULL) -- Opcional: Solo si hay algún enlace