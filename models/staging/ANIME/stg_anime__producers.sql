{{ config(
    materialized='table'
) }}

select distinct
        producer_id,
        producer_name
    from {{ ref('base_anime__producers') }}
    where producer_id is not null

