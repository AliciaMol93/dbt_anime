{{ config(materialized='table') }}

WITH favs AS (
    SELECT
        lower(trim(username)) AS username,
        lower(trim(fav_type)) AS fav_type,
        trim(id) AS entity_id,
        {{ surrogate_key(['username','fav_type','id']) }} AS id_favs,
        CURRENT_TIMESTAMP() AS last_updated_at
    FROM {{ source('anime_source', 'FAVS') }}
    WHERE username IS NOT NULL
      AND fav_type IS NOT NULL
      AND id IS NOT NULL
)
SELECT * FROM favs