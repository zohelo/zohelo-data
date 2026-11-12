{# Generic tests of the city GTFS family (gtfs_gdansk, gtfs_poznan, gtfs_krakow, gtfs_wroclaw; ingestion/common/gtfs.py).
   Each is applied in the source's own YAML to one model of that source, so no test reads another source's tables. #}

{# Completeness: every declared feed of the source has rows (a feed whose files stop loading fails here instead of
   thinning the city's totals silently). Returns the missing feeds. #}
{% test gtfs_feeds_present(model, feeds) %}
select f.feed
from (values {% for feed in feeds %}('{{ feed }}'){{ ',' if not loop.last }}{% endfor %}) as f (feed)
where f.feed not in (select feed from {{ model }})
{% endtest %}

{# Snapshot choice (poc_gtfs_service_days): a day takes the newest window of its feed downloaded on or before the day
   (Warsaw date), else the earliest window. Returns rows for which another window of the same feed holding that day
   (Bronze entity '<feed>_<first>_<last>', first <= day <= last) is the better choice by that rule. #}
{% test gtfs_snapshot_choice(model, records) %}
with windows as (
    select
        regexp_extract(entity, '^(.+)_[0-9]{8}_[0-9]{8}$', 1) as feed,
        strptime(regexp_extract(entity, '_([0-9]{8})_[0-9]{8}$', 1), '%Y%m%d')::date as first_day,
        strptime(regexp_extract(entity, '_([0-9]{8})$', 1), '%Y%m%d')::date as last_day,
        ingested_at,
        cast(timezone('Europe/Warsaw', ingested_at) as date) as download_day
    from {{ records }}
    group by all
)

select
    m.feed,
    m.service_date,
    m.route_type,
    m.snapshot_entity
from {{ model }} as m
where exists (
    select 1
    from windows as w
    where
        w.feed = m.feed
        and m.service_date between w.first_day and w.last_day
        and (
            (w.download_day <= m.service_date and w.ingested_at > m.ingested_at)
            or (m.snapshot_date > m.service_date and w.ingested_at < m.ingested_at)
        )
)
{% endtest %}

{# No service day lost between snapshots: every day from a feed's first to its last service day has rows. A gap
   means the feed was not landed while that day was in its window; severity warn, since a past gap cannot be
   repaired (the source keeps no archive). Returns the missing days. #}
{% test gtfs_service_days_contiguous(model) %}
with days as (
    select
        feed,
        min(service_date) as first_day,
        max(service_date) as last_day
    from {{ model }}
    group by all
)

select
    d.feed,
    cast(g.day as date) as service_date
from days as d,
    unnest(generate_series(d.first_day::timestamp, d.last_day::timestamp, interval 1 day)) as g (day)
where not exists (
    select 1
    from {{ model }} as m
    where m.feed = d.feed and m.service_date = cast(g.day as date)
)
{% endtest %}

{# Value bounds of a numeric column (min_value <= value <= max_value): catches unit and scale errors and runaway
   joins, not small mistakes. Returns the rows outside. #}
{% test gtfs_value_between(model, column_name, min_value, max_value) %}
select *
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
