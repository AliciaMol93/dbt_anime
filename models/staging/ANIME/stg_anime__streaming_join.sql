{{ config(
    materialized='table',
    unique_key=['anime_id', 'streaming_id'] 
) }}

with unnested_streaming as (
    SELECT
       cast({{ surrogate_key(["mal_id"]) }} as string) AS anime_id,       
        lower(trim(f.value::string)) as streaming_name_raw 
        
    FROM {{ source('anime_source', 'DETAILS') }} a,
    lateral flatten(input => PARSE_JSON(a.streaming)) f
    
    WHERE a.streaming IS NOT NULL
      AND f.value::string IS NOT NULL
      AND f.value::string <> '[]'
)

select
    us.anime_id,
    s.streaming_id,
    {{ surrogate_key(['us.anime_id', 's.streaming_id']) }} as anime_streaming_key
from unnested_streaming us
join {{ ref('stg_anime__streaming') }} s
    on us.streaming_name_raw = s.streaming_name
where us.streaming_name_raw is not null