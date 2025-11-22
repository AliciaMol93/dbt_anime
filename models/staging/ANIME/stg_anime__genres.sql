{{ config(materialized="table") }}

select distinct
    genre_id,
    genre_name
from {{ ref("base_anime__genres") }}
where genre_name is not null