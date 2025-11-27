-- Test singular: integridad del staging de anime stats (basado en tus columnas reales)

WITH invalid AS (

    SELECT
        anime_id,
        ingestion_ts,
        watching_count,
        completed_count,
        on_hold_count,
        dropped_count,
        plan_to_watch_count,
        total_users_interacting_count

    FROM {{ ref('stg_anime__stats') }}

    WHERE
        -- anime_id no puede ser null
        anime_id IS NULL

        -- ingestion timestamp debe existir
        OR ingestion_ts IS NULL

        -- ninguna métrica puede ser negativa
        OR watching_count < 0
        OR completed_count < 0
        OR on_hold_count < 0
        OR dropped_count < 0
        OR plan_to_watch_count < 0
        OR total_users_interacting_count < 0

        -- inconsistencia: total < suma de estados
        OR (
            watching_count IS NOT NULL
            AND completed_count IS NOT NULL
            AND on_hold_count IS NOT NULL
            AND dropped_count IS NOT NULL
            AND plan_to_watch_count IS NOT NULL
            AND total_users_interacting_count IS NOT NULL
            AND total_users_interacting_count < (
                watching_count 
                + completed_count 
                + on_hold_count 
                + dropped_count 
                + plan_to_watch_count
            )
        )
)

SELECT *
FROM invalid
