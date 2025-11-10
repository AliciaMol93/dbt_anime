{{
    config(
        materialized='incremental',
        unique_key='genre_id',
        on_schema_change='fail'
    )
}}

-- CTE 1: Desanidar la columna 'genres'
with unnested_genres as (
    select
        -- Se renombra a 'genre_name_raw' para consistencia con el ID
        lower(trim(f.value::string)) as genre_name_raw 
    from {{ ref('stg_anime__details') }} a,
    
    -- Usamos PARSE_JSON() para convertir el VARCHAR en un ARRAY/VARIANT y desanidar
    lateral flatten(input => PARSE_JSON(a.genres)) f
    
    -- Excluir los nulos y los arrays vacíos después del desanidamiento
    where a.genres is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),

-- CTE 2: Identificar los valores únicos y generar la clave subrogada
distinct_genres as (
    select distinct
        -- Aplicar la macro sobre la columna limpia 'genre_name_raw'
        {{ dbt_utils.generate_surrogate_key(['genre_name_raw']) }} as genre_id,
        
        -- CORRECCIÓN: Seleccionar la columna limpia generada en el CTE anterior
        genre_name_raw as genre_name 
        
    from unnested_genres
    where genre_name_raw is not null
)

-- Lógica Incremental: Insertar solo los nuevos géneros (Anti-Join)
select *
from distinct_genres d

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t
    where t.genre_id = d.genre_id
)
{% endif %}