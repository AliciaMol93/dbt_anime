{{ config(
    materialized='table',
    unique_key=['person_id', 'anime_id', 'position']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['PERSON_MAL_ID']) }} AS person_id,
    {{ dbt_utils.generate_surrogate_key(['ANIME_MAL_ID']) }} AS anime_id,
    REGEXP_REPLACE(lower(trim(position)), '\\s*\\(.*\\)', '') as position
    current_timestamp() AS last_updated_at
FROM {{ source('anime_source', 'PERSON_ANIME_WORKS') }}
WHERE PERSON_MAL_ID IS NOT NULL 
  AND ANIME_MAL_ID IS NOT NULL
