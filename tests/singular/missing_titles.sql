-- test: missing_titles.sql
-- Purpose: Detect animes missing both English and Japanese titles

select *
from {{ ref('stg_anime__titles') }}
where (title is null or trim(title) = '')
  and (title_japanese is null or trim(title_japanese) = '')
