{# News headlines (topics, docs/topics.md): one row per article, from every landed copy of every feed in
   bronze.news_items. Deduplicated first by canonical URL (scheme https, host lower case, no fragment, no
   utm_* parameters, no trailing slash), then by outlet + normalised title (lower case, letters and digits
   only) on the same publication day (recurring daily titles stay), keeping the earliest sighting.
   Kept 400 days after publication (the year ranking); title, link, outlet, language and time only:
   no article text. Grain: headline_id. #}
with items as (
    select
        entity,
        outlet,
        coalesce(lower(left(feed_lang, 2)), case when outlet = 'notes_from_poland' then 'en' else 'pl' end) as lang,
        title,
        link,
        coalesce(published_at, ingested_at) as published_at,
        published_at is null as published_from_ingest,
        ingested_at,
        raw_sha256,
        regexp_replace(trim(link), '#.*$', '') as link_nofrag
    from {{ source('news_feeds', 'news_items') }}
),

parts as (
    select
        *,
        lower(regexp_extract(link_nofrag, '^[a-z]+://([^/?]+)', 1)) as host,
        rtrim(regexp_extract(link_nofrag, '^[a-z]+://[^/?]+([^?]*)', 1), '/') as path,
        ltrim(regexp_replace(regexp_extract(link_nofrag, '\?(.*)$', 1), '(^|&)utm_[^&]*', '', 'g'), '&') as query
    from items
),

canonical as (
    select
        *,
        'https://' || host || path || case when query != '' then '?' || query else '' end as canonical_url,
        trim(regexp_replace(lower(title), '[^[:alnum:]]+', ' ', 'g')) as title_key
    from parts
),

by_url as (
    select
        canonical_url,
        arg_min(outlet, ingested_at) as outlet,
        arg_min(lang, ingested_at) as lang,
        arg_min(title, ingested_at) as title,
        arg_min(link, ingested_at) as url,
        arg_min(title_key, ingested_at) as title_key,
        min(published_at) as published_at,
        bool_and(published_from_ingest) as published_from_ingest,
        min(ingested_at) as first_seen_at,
        string_agg(distinct entity, ';' order by entity) as feeds,
        arg_min(raw_sha256, ingested_at) as raw_sha256
    from canonical
    group by canonical_url
),

by_title as (
    select *
    from by_url
    qualify row_number() over (
        partition by outlet, title_key, cast(published_at as date) order by first_seen_at, canonical_url
    ) = 1
)

select
    md5(canonical_url) as headline_id,
    outlet as outlet_key,
    lang,
    title,
    url,
    canonical_url,
    published_at,
    published_from_ingest,
    first_seen_at,
    feeds,
    raw_sha256
from by_title
where published_at >= current_date - interval 400 day
