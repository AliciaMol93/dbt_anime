{{ config(materialized="table") }}

with
    base_streaming as (

        select
            {{ surrogate_key(["mal_id"]) }} as anime_id,
            f.value::string as streaming_name_raw,
        from
            {{ source("anime_source", "DETAILS") }} a,
            lateral flatten(input => parse_json(a.streaming)) f
        where
            a.streaming is not null
            and f.value::string is not null
            and f.value::string <> '[]'
            and streaming_name_raw is not null
    )
select
    anime_id,
    lower(trim(streaming_name_raw)) as streaming_name,
    {{ surrogate_key(["streaming_name_raw"]) }} as streaming_id
from base_streaming
