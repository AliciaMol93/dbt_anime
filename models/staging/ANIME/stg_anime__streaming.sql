{{
    config(
        materialized='incremental',
        unique_key='streaming_id',
        on_schema_change='fail' 
    )
}}

-- CTE 1: Desanidar la columna 'streaming'
with unnested_streaming as (
    select
        -- La columna 'value' contiene la plataforma individual después del FLATTEN
        lower(trim(f.value::string)) as streaming_name_raw 
    from {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.streaming)) f
    where a.streaming is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),

-- CTE 2: Identificar los valores únicos y generar la clave subrogada

distinct_streaming as (
    select distinct
        {{ surrogate_key(['streaming_name_raw']) }} as streaming_id,
        streaming_name_raw as streaming_name
    from unnested_streaming
    where streaming_name_raw is not null
)

{% if is_incremental() %}


,final_streaming as (
    select
        t1.streaming_id,
        t1.streaming_name
    from distinct_streaming t1
    
    left join {{ this }} t2 
        on t1.streaming_id = t2.streaming_id

    where t2.streaming_id is null

select * from final_streaming

{% else %}

select
    streaming_id,
    streaming_name
from distinct_streaming

{% endif %}