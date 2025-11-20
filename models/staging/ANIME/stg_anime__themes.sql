{{ config(
    materialized='incremental',
    unique_key='theme_id'
) }}

with unnested_themes as (
    select
        lower(trim(f.value::string)) as theme_name_raw
    from {{ source('anime_source', 'DETAILS') }} a,
         lateral flatten(input => parse_json(a.themes)) f
    where a.themes is not null
      and f.value::string is not null
      and f.value::string <> '[]'
),

distinct_themes as (
    select distinct
        {{ surrogate_key(['theme_name_raw']) }} as theme_id,
        theme_name_raw as theme_name
    from unnested_themes
    where theme_name_raw is not null
)

select *
from distinct_themes t1

{% if is_incremental() %}
where t1.theme_id not in (select theme_id from {{ this }})
{% endif %}
