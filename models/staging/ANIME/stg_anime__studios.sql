{{
    config(
        materialized='incremental',
        unique_key='studio_id',
        on_schema_change='fail' 
    )
}}

-- CTE 1: Desanidar la columna 'studios' para obtener una fila por cada estudio
with unnested_studios as (
    select
        -- La columna 'value' contiene el nombre del estudio individual
        lower(trim(f.value::string)) as studio_name_raw 
    from {{ ref('stg_anime__details') }} a,
    
    -- Usamos PARSE_JSON() para convertir el VARCHAR en un ARRAY/VARIANT
    lateral flatten(input => PARSE_JSON(a.studios)) f
    
    -- Excluir nulos, arrays vacíos y cualquier valor nulo resultante del unnest
    where a.studios is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),

-- CTE 2: Identificar los valores únicos y generar la clave subrogada
distinct_studios as (
    select distinct
        -- CORRECCIÓN: Usar la macro estándar y el nombre de columna limpio
        {{ dbt_utils.generate_surrogate_key(['studio_name_raw']) }} as studio_id,
        studio_name_raw as studio_name -- Referenciar la columna limpia del CTE anterior
    from unnested_studios -- Leer del CTE correcto
    where studio_name_raw is not null
)

-- Lógica Incremental: Insertar solo los nuevos estudios (Anti-Join)
{% if is_incremental() %}

final_studios as (
    select
        t1.studio_id,
        t1.studio_name
    from distinct_studios t1
    
    -- Unimos con la tabla de destino existente ({{ this }})
    left join {{ this }} t2 
        on t1.studio_id = t2.studio_id

    -- Mantenemos SOLAMENTE los estudios que NO tienen una coincidencia (son nuevos)
    where t2.studio_id is null
)

select * from final_studios

{% else %}

-- Si es el primer run o un full-refresh, insertamos todos los datos directamente
select
    studio_id,
    studio_name
from distinct_studios

{% endif %}