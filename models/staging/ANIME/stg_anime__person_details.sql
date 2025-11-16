{{ config(
    materialized='table',
    unique_key=['person_id']
) }}

WITH src_person_details AS (
    SELECT
        PERSON_MAL_ID AS person_mal_id,
        NULLIF(LOWER(TRIM(NAME)), '') AS name,
        NULLIF(LOWER(TRIM(GIVEN_NAME)), '') AS given_name,
        NULLIF(LOWER(TRIM(FAMILY_NAME)), '') AS family_name,
        CAST(BIRTHDAY AS DATE) AS birthday,
        CAST(FAVORITES AS INT) AS favorites
    FROM {{ source('anime_source', 'PERSON_DETAILS') }}
    WHERE PERSON_MAL_ID IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['person_mal_id']) }} AS person_id,
    name,
    given_name,
    family_name,
    birthday,
    favorites,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM src_person_details
