{# Announced releases of official statistics (topics, docs/topics.md) from bronze.release_calendar_items (newest
   copy of each calendar file): Eurostat euro indicators, GUS kalendarium, ECB statistical and meeting calendars.
   kpi_keys = the v2 KPIs the release publishes (seed release_kpi by title; Eurostat also by dataset code against
   kpi_v2_map), ';' separated, empty when none. Grain: release_id (md5 of publisher, date, title). #}
with items as (
    select
        publisher,
        cast(release_date as date) as release_date,
        release_time,
        title,
        url,
        reference_period,
        dataset_codes,
        tentative = 'true' as tentative,
        raw_sha256,
        md5(publisher || '|' || release_date || '|' || title) as release_id
    from {{ source('release_calendars', 'release_calendar_items') }}
    qualify row_number() over (
        partition by publisher, release_date, title order by ingested_at desc, raw_sha256 asc
    ) = 1
),

by_title as (
    select
        i.release_id,
        r.kpi_key
    from items as i
    inner join {{ ref('release_kpi') }} as r
        on i.publisher = r.publisher and regexp_matches(lower(i.title), r.pattern)
),

by_dataset as (
    select
        i.release_id,
        m.v2_key as kpi_key
    from items as i
    cross join unnest(string_split(lower(coalesce(i.dataset_codes, '')), ',')) as c (code)
    inner join {{ ref('kpi_v2_map') }} as m
        on
            m.source_key = 'eurostat'
            and list_contains(string_split(lower(m.dataset), ';'), trim(c.code))
    where i.publisher = 'eurostat'
),

links as (
    select
        release_id,
        string_agg(distinct kpi_key, ';' order by kpi_key) as kpi_keys
    from (
        select
            release_id,
            kpi_key
        from by_title
        union
        select
            release_id,
            kpi_key
        from by_dataset
    ) as linked
    group by release_id
)

select
    i.release_id,
    i.publisher,
    i.release_date,
    i.release_time,
    i.title,
    i.url,
    i.reference_period,
    i.dataset_codes,
    i.tentative,
    coalesce(l.kpi_keys, '') as kpi_keys,
    i.raw_sha256
from items as i
left join links as l on i.release_id = l.release_id
