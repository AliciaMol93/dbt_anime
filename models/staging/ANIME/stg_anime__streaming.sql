{{ config(
    materialized='table'
) }}

select
    streaming_id,
    streaming_name
from {{ ref("base_anime__streaming") }}
where streaming_name is not null
