{{ config(materialized="table") }}

select
    anime_id,
    licensor_id,
    {{ surrogate_key(["anime_id", "licensor_id"]) }} as anime_licensor_key
from {{ ref('base_anime__licensors') }}
where licensor_name is not null

