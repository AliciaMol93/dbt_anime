{{ config(
    materialized='table',
    unique_key=['person_id', 'anime_id', 'position']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['PERSON_MAL_ID']) }} as person_id,
    {{ dbt_utils.generate_surrogate_key(['ANIME_MAL_ID']) }} as anime_id,
    lower(trim(POSITION)) AS position
    
FROM {{ source('anime_source', 'PERSON_ANIME_WORKS') }}
WHERE 
    PERSON_MAL_ID IS NOT NULL 
    AND ANIME_MAL_ID IS NOT NULL
