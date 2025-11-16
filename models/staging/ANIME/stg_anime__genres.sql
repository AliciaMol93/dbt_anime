{{ config(
    materialized='table', 
    unique_key='genre_id',

) }}

with unnested_genres as (
    select
        lower(trim(f.value::string)) as genre_name_raw -- Un género por fila (Ej: 'action')
    from {{ ref('stg_anime__details') }} a,
    
    -- Usamos PARSE_JSON() para convertir el VARCHAR en un ARRAY/VARIANT y desanidar
    lateral flatten(input => PARSE_JSON(a.genres)) f
    
    where a.genres is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),

-- 2. Identificar los valores únicos y generar la clave subrogada
distinct_genres as (
    select distinct
        {{ surrogate_key(['genre_name_raw']) }} as genre_id,
        
        genre_name_raw as genre_name 
        
    from unnested_genres
    where genre_name_raw is not null
)

SELECT * FROM distinct_genres
-- NOTA: Quita la lógica incremental por ahora para que solo se ejecute como TABLE