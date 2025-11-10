{{
    config(
        materialized="incremental",
        unique_key=["theme_id"],
        on_schema_change="fail"
    )
}}

-- CTE 1: Desanidar la columna 'themes'
with unnested_themes as (
    select
        -- La columna 'value' contiene el nombre del tema individual (limpio)
        lower(trim(f.value::string)) as theme_name_raw 
    from {{ ref('stg_anime__details') }} a,
    
    -- Usamos PARSE_JSON() para convertir el VARCHAR en un ARRAY/VARIANT
    lateral flatten(input => PARSE_JSON(a.themes)) f
    
    -- Excluir los nulos y los arrays vacíos después del desanidamiento
    where a.themes is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),

-- CTE 2: Identificar los valores únicos y generar una ID consistente (Clave Subrogada)
distinct_themes as (
    select distinct
        -- CORRECCIÓN 1: Usar dbt_utils.generate_surrogate_key
        -- CORRECCIÓN 2: Aplicar la macro sobre la columna limpia 'theme_name_raw'
        {{ dbt_utils.generate_surrogate_key(['theme_name_raw']) }} as theme_id,
        
        -- CORRECCIÓN 3: Seleccionar la columna limpia generada en el CTE anterior
        theme_name_raw as theme_name
        
    from unnested_themes
    where theme_name_raw is not null -- Usar la columna limpia para filtrar nulos
)

-- Lógica Incremental: Insertar solo los nuevos temas (Anti-Join)
{% if is_incremental() %}

final_themes as (
    select
        t1.theme_id,
        t1.theme_name
    from distinct_themes t1
    
    -- Unimos la nueva lista de temas (t1) con la tabla de destino existente (t2)
    left join {{ this }} t2 
        on t1.theme_id = t2.theme_id

    -- Mantenemos SOLAMENTE los temas que NO tienen una coincidencia (son nuevos)
    where t2.theme_id is null
)

select * from final_themes

{% else %}

-- Si es el primer run o un full-refresh, insertamos todos los datos directamente
select
    theme_id,
    theme_name
from distinct_themes

{% endif %}