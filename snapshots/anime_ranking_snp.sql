{% snapshot anime_ranking_snp %}
    {{
        config(
            target_schema="snapshots",
            unique_key="anime_id",
            strategy="check",
            check_cols=["score", "rank", "popularity", "members", "favorites"],
        )
    }}
    select
        anime_id,
        rank,
        score,
        scored_by,
        popularity,
        members,
        favorites
    FROM {{ ref('stg_anime__details') }}

{% endsnapshot %}
