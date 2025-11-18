{{
    config(
        materialized='incremental',
        unique_key='studio_id',
        on_schema_change='fail' 
    )
}}


with unnested_studios as (
    select
        lower(trim(f.value::string)) as studio_name_raw 
    from {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.studios)) f
    
    where a.studios is not null
      and f.value::string <> '[]'
      and f.value::string is not null
),


distinct_studios as (
    select distinct
        
        {{ surrogate_key(['studio_name_raw']) }} as studio_id,
        studio_name_raw as studio_name 
    from unnested_studios
    where studio_name_raw is not null
)

{% if is_incremental() %}

final_studios as (
    select
        t1.studio_id,
        t1.studio_name
    from distinct_studios t1
    
    left join {{ this }} t2 
        on t1.studio_id = t2.studio_id

    where t2.studio_id is null
)

select * from final_studios

{% else %}

select
    studio_id,
    studio_name
from distinct_studios

{% endif %}