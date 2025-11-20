{{ config(
    materialized='table'
) }}

WITH src_links AS (
    SELECT
        PERSON_MAL_ID AS person_mal_id,
        NULLIF((TRIM(URL)), '') AS mal_url,
        NULLIF(TRIM(IMAGE_URL)), '') AS image_url,
        NULLIF(TRIM(WEBSITE_URL)), '') AS website_url
    FROM {{ source('anime_source', 'PERSON_DETAILS') }} 
    WHERE PERSON_MAL_ID IS NOT NULL
)

SELECT
    {{ surrogate_key(["person_mal_id"]) }} AS person_id,
    person_mal_id,
    mal_url,
    image_url,
    website_url,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM src_links
WHERE mal_url IS NOT NULL OR image_url IS NOT NULL OR website_url IS NOT NULL
