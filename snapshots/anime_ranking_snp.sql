{% snapshot anime_ranking_snp %}
{{
    config(
        target_schema="SNAPSHOTS",
        unique_key="anime_id",
        strategy="check",
        check_cols=[
            "rank",
            "score",
            "scored_by",
            "popularity",
            "members",
            "favorites"
        ]
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
