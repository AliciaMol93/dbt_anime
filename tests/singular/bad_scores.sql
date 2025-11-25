-- test: bad_scores.sql
-- Purpose: Detect invalid scoring values in stg_anime__scores

select *
from {{ ref('stg_anime__scores') }}
where score < 1
   or score > 10
   or votes < 0
   or percentage < 0
   or percentage > 100
