{{
    config(
        materialized="table"
    )
}}

select distinct 
    studio_id, 
    studio_name
from {{ ref("base_anime__studios") }}
where studio_name is not null
