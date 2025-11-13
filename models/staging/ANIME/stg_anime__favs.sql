{{ config(materialized='table') }}

SELECT
    lower(trim(username)) AS username,
    lower(trim(fav_type)) AS fav_type,
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS entity_id
FROM {{ source('anime_source', 'FAVS') }}
WHERE username IS NOT NULL AND fav_type IS NOT NULL AND id IS NOT NULL
