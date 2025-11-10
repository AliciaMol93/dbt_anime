{{
    config(
        materialized='incremental',
        unique_key='producer_id',
        on_schema_change='fail' 
    )
}}

-- CTE 1: Desanidar la columna 'producers'
with unnested_producers as (
    select
        -- La columna 'value' contiene el nombre del productor individual
        lower(trim(f.value::string)) as producer_name_raw 
    from {{ ref('stg_anime__details') }} a,
    
    -- Usamos PARSE_JSON() para convertir el VARCHAR en un ARRAY/VARIANT y desanidar
    lateral flatten(input => PARSE_JSON(a.producers)) f
    
    -- Excluir nulos, arrays vacíos
    where a.producers is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),

-- CTE 2: Identificar los valores únicos y generar la clave subrogada
distinct_producers as (
    select distinct
        -- Aplicar la macro sobre la columna limpia 'producer_name_raw'
        {{ dbt_utils.generate_surrogate_key(['producer_name_raw']) }} as producer_id,
        
        -- Seleccionar la columna limpia generada en el CTE anterior
        producer_name_raw as producer_name
    from unnested_producers
    where producer_name_raw is not null
),

-- Lógica Incremental: Insertar solo los nuevos productores (Anti-Join)
{% if is_incremental() %}

final_producers as (
    select
        t1.producer_id,
        t1.producer_name
    from distinct_producers t1
    
    -- Unimos con la tabla de destino existente ({{ this }})
    left join {{ this }} t2 
        on t1.producer_id = t2.producer_id

    -- Mantenemos SOLAMENTE los nuevos productores
    where t2.producer_id is null
)

select * from final_producers

{% else %}

-- Full-refresh
select
    producer_id,
    producer_name
from distinct_producers

{% endif %}