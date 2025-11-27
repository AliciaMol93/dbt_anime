{{
    config(
        materialized="table"
    )
}}

select
    anime_id,
    streaming_id,
    {{ surrogate_key(["anime_id", "streaming_id"]) }} as anime_streaming_key

from {{ ref("base_anime__streaming") }}
where streaming_name is not null
