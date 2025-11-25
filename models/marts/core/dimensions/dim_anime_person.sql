
{{ config(materialized='table', tags=['marts','dimension']) }}

select
    pd.person_id,
    name,
    given_name,
    family_name,
    birthday,
    favorites,
    mal_url,
    image_url,
    website_url,
    narrative_about_text
from {{ ref('stg_anime__person_details') }} pd
left join {{ ref('stg_anime__person_links') }} pl on pd.person_id = pl.person_id
left join {{ ref('stg_anime__person_about') }} pa on pd.person_id = pa.person_id
