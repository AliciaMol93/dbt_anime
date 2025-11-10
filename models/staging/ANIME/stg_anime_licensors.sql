{{
    config(
        materialized='incremental',
        unique_key='licensor_id',
        on_schema_change='fail' 
    )
}}

-- CTE 1: Desanidar la columna 'licensors'
with unnested_licensors as (
    select
        -- Esta columna es la que contiene el nombre limpio de CADA licenciatario
        lower(trim(f.value::string)) as licensor_name_raw 
    from {{ ref('stg_anime__details') }} a,
    
    -- Usamos PARSE_JSON() para convertir el VARCHAR en un ARRAY/VARIANT y desanidar
    lateral flatten(input => PARSE_JSON(a.licensors)) f
    
    -- Excluir los nulos y los arrays vacíos después del desanidamiento
    where a.licensors is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),

-- CTE 2: Identificar los valores únicos y generar una ID consistente (Clave Subrogada)
distinct_licensors as (
    select distinct
        -- CORRECCIÓN 1: Aplicar la macro sobre la columna limpia 'licensor_name_raw'
        {{ dbt_utils.generate_surrogate_key(['licensor_name_raw']) }} as licensor_id,
        
        -- CORRECCIÓN 2: Seleccionar la columna limpia generada en el CTE anterior
        licensor_name_raw as licensor_name
        
    from unnested_licensors
    where licensor_name_raw is not null -- Filtrar nulos sobre la columna limpia
),

{% if is_incremental() %}

final_licensors as (
    select
        t1.licensor_id,
        t1.licensor_name
    from distinct_licensors t1
    
    -- Unimos con la tabla de destino existente ({{ this }})
    left join {{ this }} t2 
        on t1.licensor_id = t2.licensor_id

    -- Mantenemos SOLAMENTE los nuevos licenciatarios (Anti-Join)
    where t2.licensor_id is null
)

select * from final_licensors

{% else %}

-- Si es el primer run o un full-refresh
select
    licensor_id,
    licensor_name
from distinct_licensors

{% endif %}