{% snapshot anime_ranking_snp %}
{{
    config(
        target_schema="snapshots",
        unique_key="anime_id",   
        strategy="check",
        check_cols=["score", "rank", "popularity", "members", "favorites"]
    )
}}
select
    CAST(anime_id AS VARCHAR) AS anime_id,  
    rank,
    score,
    scored_by,
    popularity,
    members,
    favorites
from {{ ref('stg_anime__details') }}
{% endsnapshot %}
