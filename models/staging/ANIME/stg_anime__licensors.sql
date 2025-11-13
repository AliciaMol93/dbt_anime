{{
    config(
        materialized="incremental", unique_key="licensor_id", on_schema_change="fail"
    )
}}

-- CTE 1: Desanidar la columna 'licensors'
with
    unnested_licensors as (
        select lower(trim(f.value::string)) as licensor_name_raw
        from
            {{ ref("stg_anime__details") }} a,
            lateral flatten(input => parse_json(a.licensors)) f
        where
            a.licensors is not null
            and f.value::string <> '[]'
            and f.value::string is not null
    ),

    -- CTE 2: Identificar los valores únicos y generar una ID consistente
    distinct_licensors as (
        select distinct
            {{ dbt_utils.generate_surrogate_key(["licensor_name_raw"]) }}
            as licensor_id,
            licensor_name_raw as licensor_name
        from unnested_licensors
        where licensor_name_raw is not null
    )

select licensor_id, licensor_name
from distinct_licensors

{% if is_incremental() %}
    -- LÓGICA INCREMENTAL: SÓLO SE AÑADEN NUEVOS LICENCIATARIOS
    -- Esto se compila sólo si ya existe la tabla final ({{ this }})
    where licensor_id not in (select licensor_id from {{ this }})
{% endif %}
