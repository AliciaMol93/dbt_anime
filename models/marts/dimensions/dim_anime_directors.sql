{{ config(materialized="table") }}

select
    paw.anime_id,
    pd.person_id,
    pd.name as director_name
from {{ ref('stg_anime__person_anime_works') }} paw
join {{ ref('stg_anime__person_details') }} pd
    on paw.person_id = pd.person_id
where paw.position ilike '%director%'
