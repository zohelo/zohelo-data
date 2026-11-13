{# gtfs_gdansk (register PL-ENV-029): Gold registration (D34, docs/source-registration.md), and the Silver and Gold SQL
   shared by the city GTFS family (gtfs_gdansk, gtfs_poznan, gtfs_krakow, gtfs_wroclaw; ingestion/common/gtfs.py).
   The shared macros live here because a per-source macro file counts only for the models that call it
   (warehouse/changed.py); a file under macros/ outside sources/ would rebuild every model of the project. #}
{% macro poc_source__gtfs_gdansk() %}
    {{ return({'block_model': 'poc_build_gtfs_gdansk', 'blocks': [['scheduled_trips', 'Scheduled public transport trips per service day (GTFS, ZTM Gdańsk)']]}) }}
{% endmacro %}

{# The feed is ZTM Gdańsk's timetable; its lines reach neighbouring gminas, the area is the city that publishes it. #}
{% macro poc_geography__gtfs_gdansk() %}
gtfs_gdansk as (
    select
        'gtfs_gdansk' as source_system,
        'Gdańsk' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:2261011'
)
{% endmacro %}

{# The feed of a Landing entity '<feed>_<first>_<last>' (ingestion/common/gtfs.py). #}
{% macro poc_gtfs_feed(entity) -%}
    regexp_extract({{ entity }}, '^(.+)_[0-9]{8}_[0-9]{8}$', 1)
{%- endmacro %}

{# The Bronze rows of the newest landed window of each feed: the current network (stops, routes). #}
{% macro poc_gtfs_current(records, gtfs_file) %}
    select r.*
    from {{ records }} as r
    inner join (
        select
            {{ poc_gtfs_feed('entity') }} as feed,
            max(ingested_at) as ingested_at
        from {{ records }}
        group by all
    ) as n on {{ poc_gtfs_feed('r.entity') }} = n.feed and r.ingested_at = n.ingested_at
    where r.gtfs_file = '{{ gtfs_file }}'
{% endmacro %}

{# Silver stops: one row per feed x stop_id of the newest window of each feed, values typed, codes as published. #}
{% macro poc_gtfs_stops(records) %}
with c as ({{ poc_gtfs_current(records, 'stops.txt') }})

select
    {{ poc_gtfs_feed('entity') }} as feed,
    stop_id,
    stop_code,
    stop_name,
    cast(stop_lat as double) as stop_lat,
    cast(stop_lon as double) as stop_lon,
    cast(coalesce(location_type, '0') as integer) as location_type,
    parent_station,
    entity as snapshot_entity,
    source_version,
    ingested_at,
    raw_sha256
from c
{% endmacro %}

{# Silver routes: one row per feed x route_id of the newest window of each feed. #}
{% macro poc_gtfs_routes(records) %}
with c as ({{ poc_gtfs_current(records, 'routes.txt') }})

select
    {{ poc_gtfs_feed('entity') }} as feed,
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    cast(route_type as integer) as route_type,
    entity as snapshot_entity,
    source_version,
    ingested_at,
    raw_sha256
from c
{% endmacro %}

{# The calendar day (Europe/Warsaw, the feeds' agency_timezone) of a UTC timestamp. #}
{% macro poc_gtfs_local_day(ts) -%}
    cast(timezone('Europe/Warsaw', {{ ts }}) as date)
{%- endmacro %}

{# Silver scheduled service per day: one row per feed x service_date x route_type with the trips that run that day,
   their stop_times rows (departures) and routes. Service days: calendar.txt (weekday flags from start_date to
   end_date) plus calendar_dates.txt dates of exception_type 1, minus those of exception_type 2; a feed may have
   either file or both. Several landed windows '<feed>_<first>_<last>' of a feed can hold a day (a rolling feed is
   landed again and again). Which snapshot wins: the newest one downloaded on or before that day (Warsaw date of
   ingested_at), i.e. the timetable in force when the day ran; a day before the first download takes the earliest
   snapshot. So a day that has passed never changes when a later feed overlaps it, a future day follows the newest
   download, and no day is counted twice. snapshot_date = the download day of the chosen snapshot; days after the
   feed's newest download are the published plan, kept here and not loaded into Gold. snapshots_covering counts the windows that held the day. Two
   windows of a feed landed at the same instant would both be kept and fail the unique test (never a silent pick). #}
{% macro poc_gtfs_service_days(records) %}
with r as (select * from {{ records }}),

snapshots as (
    select
        entity,
        raw_sha256,
        source_version,
        ingested_at,
        {{ poc_gtfs_feed('entity') }} as feed,
        strptime(regexp_extract(entity, '_([0-9]{8})_[0-9]{8}$', 1), '%Y%m%d')::date as first_day,
        strptime(regexp_extract(entity, '_([0-9]{8})$', 1), '%Y%m%d')::date as last_day
    from r
    group by all
),

covering as (
    select
        s.feed,
        cast(d.day as date) as service_date,
        s.raw_sha256,
        s.ingested_at
    from snapshots as s,
        unnest(generate_series(s.first_day::timestamp, s.last_day::timestamp, interval 1 day)) as d (day)
),

pick as (
    select
        feed,
        service_date,
        coalesce(
            max(ingested_at) filter (where {{ poc_gtfs_local_day('ingested_at') }} <= service_date),
            min(ingested_at)
        ) as ingested_at,
        count(*) as snapshots_covering
    from covering
    group by all
),

chosen as (
    select
        c.feed,
        c.service_date,
        c.raw_sha256,
        p.snapshots_covering
    from covering as c
    inner join pick as p
        on c.feed = p.feed and c.service_date = p.service_date and c.ingested_at = p.ingested_at
),

calendar as (
    select
        raw_sha256,
        service_id,
        strptime(start_date, '%Y%m%d')::timestamp as start_day,
        strptime(end_date, '%Y%m%d')::timestamp as end_day,
        [monday, tuesday, wednesday, thursday, friday, saturday, sunday] as weekdays
    from r
    where gtfs_file = 'calendar.txt'
),

calendar_days as (
    select
        c.raw_sha256,
        c.service_id,
        cast(d.day as date) as service_date
    from calendar as c,
        unnest(generate_series(c.start_day, c.end_day, interval 1 day)) as d (day)
    where c.weekdays[isodow(d.day)] = '1'
),

exceptions as (
    select
        raw_sha256,
        service_id,
        strptime("date", '%Y%m%d')::date as service_date,
        exception_type
    from r
    where gtfs_file = 'calendar_dates.txt'
),

added as (
    select raw_sha256, service_id, service_date from calendar_days
    union
    select raw_sha256, service_id, service_date from exceptions where exception_type = '1'
),

service_days as (
    select raw_sha256, service_id, service_date from added
    except
    select raw_sha256, service_id, service_date from exceptions where exception_type = '2'
),

trips as (
    select raw_sha256, trip_id, route_id, service_id from r where gtfs_file = 'trips.txt'
),

calls as (
    select raw_sha256, trip_id, stop_times_rows from r where gtfs_file = 'stop_times.txt'
),

routes as (
    select raw_sha256, route_id, route_type from r where gtfs_file = 'routes.txt'
),

daily as (
    select
        sd.raw_sha256,
        sd.service_date,
        cast(ro.route_type as integer) as route_type,
        count(*) as trips,
        coalesce(sum(ca.stop_times_rows), 0) as departures,
        count(distinct t.route_id) as routes,
        count(*) filter (where ca.trip_id is null) as trips_without_stop_times
    from service_days as sd
    inner join trips as t on sd.raw_sha256 = t.raw_sha256 and sd.service_id = t.service_id
    left join routes as ro on t.raw_sha256 = ro.raw_sha256 and t.route_id = ro.route_id
    left join calls as ca on t.raw_sha256 = ca.raw_sha256 and t.trip_id = ca.trip_id
    group by all
)

select
    ch.feed,
    d.service_date,
    d.route_type,
    d.trips,
    d.departures,
    d.routes,
    d.trips_without_stop_times,
    ch.snapshots_covering,
    s.entity as snapshot_entity,
    {{ poc_gtfs_local_day('s.ingested_at') }} as snapshot_date,
    s.source_version,
    s.ingested_at,
    ch.raw_sha256
from chosen as ch
inner join daily as d on ch.raw_sha256 = d.raw_sha256 and ch.service_date = d.service_date
inner join snapshots as s on ch.raw_sha256 = s.raw_sha256 and ch.feed = s.feed
{% endmacro %}

{# Gold build stage of one city (playbook section 7): scheduled trips per service day (frequency D) by GTFS route type
   (breakdown route_type, codes as published) and in total (is_total; the sum over route types and, in Kraków, over
   the three feeds), unit trips (D33). The area is the city that publishes the feed (poc_geography__<source>). Only
   days that every feed of the city covers are loaded (from the latest first day to the earliest last day over the
   feeds), so a city total never sums a partial set of feeds; and only days up to the newest download of every feed:
   a later day is still a plan (calendars run to 2028 in Kraków) and stays in Silver, it enters Gold once a download
   on or after that day has been seen. A day keeps the last timetable seen for it (Silver's snapshot choice). #}
{% macro poc_gtfs_build(source_system, service_days, city, authority) %}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = '{{ source_system }}'
),

feeds as (
    select
        feed,
        min(service_date) as first_day,
        least(max(service_date), max(snapshot_date)) as last_day
    from {{ service_days }}
    group by all
),

days as (
    select s.*
    from {{ service_days }} as s
    where s.service_date between (select max(first_day) from feeds) and (select min(last_day) from feeds)
),

by_type as (
    select
        service_date,
        cast(route_type as varchar) as route_type,
        sum(trips) as trips,
        max(source_version) as source_version,
        string_agg(distinct raw_sha256, ',' order by raw_sha256) as raw_sha256
    from days
    group by all
),

total as (
    select
        service_date,
        cast(null as varchar) as route_type,
        sum(trips) as trips,
        max(source_version) as source_version,
        string_agg(distinct raw_sha256, ',' order by raw_sha256) as raw_sha256
    from days
    group by all
),

observations as (
    select * from by_type
    union all
    select * from total
)

select
    '{{ source_system }}' as source_system,
    'scheduled_trips' as block_id,
    'Scheduled public transport trips per service day (GTFS, {{ authority }})' as block_name,
    '{{ source_system }}:scheduled_trips:trips' as measure_key,
    'Scheduled public transport trips per service day, {{ authority }} timetable (GTFS)' as measure_name,
    'trips' as measure_codes,
    'trips' as unit_code,
    'trips' as unit_name,
    '{{ city }}' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    o.service_date as date_key,
    strftime(o.service_date, '%Y-%m-%d') as period_label,
    case when o.route_type is null then '{}' else '{"route_type": "' || o.route_type || '"}' end as breakdown_json,
    case
        when o.route_type is not null
            then cast(to_json(map(['route_type'], [{{ poc_gtfs_route_type_name('o.route_type') }}])) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.route_type is null as is_total,
    cast(o.trips as double) as value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
left join geo as g on g.source_code = '{{ city }}'
{% endmacro %}

{# GTFS route_type codes (https://gtfs.org/documentation/schedule/reference/#routestxt) → English names. #}
{% macro poc_gtfs_route_type_name(code) -%}
    case {{ code }}
        when '0' then 'Tram'
        when '1' then 'Metro'
        when '2' then 'Rail'
        when '3' then 'Bus'
        when '4' then 'Ferry'
        when '5' then 'Cable tram'
        when '6' then 'Aerial lift'
        when '7' then 'Funicular'
        when '11' then 'Trolleybus'
        when '12' then 'Monorail'
        else 'route_type ' || {{ code }}
    end
{%- endmacro %}

{# Gold entity tables (playbook section 7, reference source): the stops and routes of each feed's newest window, keyed
   by the source's own ids '<source_system>:<feed>:<id>'; city_geography_key = the city that publishes the feed. #}
{% macro poc_gtfs_stop_entities(source_system, stops, terc) %}
select
    '{{ source_system }}:' || feed || ':' || stop_id as stop_key,
    '{{ source_system }}' as source_system,
    feed,
    stop_id,
    stop_code,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    parent_station,
    'TERC:{{ terc }}' as city_geography_key,
    snapshot_entity,
    source_version,
    raw_sha256
from {{ stops }}
{% endmacro %}

{% macro poc_gtfs_route_entities(source_system, routes, terc) %}
select
    '{{ source_system }}:' || feed || ':' || route_id as route_key,
    '{{ source_system }}' as source_system,
    feed,
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_type,
    {{ poc_gtfs_route_type_name('cast(route_type as varchar)') }} as route_type_name,
    'TERC:{{ terc }}' as city_geography_key,
    snapshot_entity,
    source_version,
    raw_sha256
from {{ routes }}
{% endmacro %}
