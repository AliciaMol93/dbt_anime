{% snapshot anime_ranking_snp %}
{{
    config(
        target_schema="snapshots",
        unique_key="anime_id",   
        strategy="timestamp",
        updated_at="ingestion_ts"
    )
}}
select
    anime_id,  
    rank,
    score,
    scored_by,
    popularity,
    members,
    favorites,
    ingestion_ts
from {{ ref('stg_anime__details') }}
where score is not null
{% endsnapshot %}
