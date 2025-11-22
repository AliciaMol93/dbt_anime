{{ config(materialized="table") }}

select distinct
    licensor_id,
    licensor_name
from {{ ref("base_anime__licensors") }}
where licensor_name is not null