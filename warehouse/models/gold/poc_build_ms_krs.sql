{{ config(alias='build_ms_krs', tags=['gold_block']) }}
{# Gold build stage: Open KRS API, entity-snapshot facts over what is loaded (silver.ms_krs_entities with a
   current extract). Two measures, one frequency each:
   ms_krs:lei_selection:entities (D, one date: the day of the newest request) = entities loaded;
   ms_krs:lei_selection:registered (M, date = first day of the month of registration in KRS) = entities loaded
   by the month they were entered in the register (an entity without a registration date adds no row; a month
   without registrations has no row, never a zero).
   Both by legal form (breakdown legal_form as the register prints it, _T all, _U not stated) and seat
   voivodeship (the register's name → poc_map_geography section ms_krs; PL = all entities loaded, wherever the
   seat; the seat is today's, also for the month of registration). The selection is the KRS numbers GLEIF lists,
   loaded over several runs: these are counts of loaded entities that still have a current extract, not of
   Polish companies and not of all registrations of a month. An extract without a voivodeship adds no
   voivodeship row; a name that is no voivodeship of dim_geography gets geography_key _X and keeps the name as
   its source code (its own series, never merged with another name). #}
with entities as (
    select
        coalesce(legal_form, '_U') as legal_form,
        seat_voivodeship,
        cast(date_trunc('month', registered_on) as date) as registered_month,
        requested_on,
        source_version,
        raw_sha256
    from {{ ref('ms_krs_entities') }}
    where has_current_extract
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'ms_krs'
),

located as (
    select
        'PL' as geo_code,
        'PL' as geography_key,
        legal_form,
        registered_month
    from entities
    union all
    select
        e.seat_voivodeship as geo_code,
        coalesce(g.geography_key, '_X') as geography_key,
        e.legal_form,
        e.registered_month
    from entities as e
    left join geo_map as g on e.seat_voivodeship = g.source_code
    where e.seat_voivodeship is not null
),

counted as (
    select
        geo_code,
        geography_key,
        legal_form,
        count(*) as entities
    from located
    group by geo_code, geography_key, legal_form
    union all
    select
        geo_code,
        geography_key,
        '_T' as legal_form,
        count(*) as entities
    from located
    group by geo_code, geography_key
),

registered as (
    select
        geo_code,
        geography_key,
        legal_form,
        registered_month,
        count(*) as entities
    from located
    where registered_month is not null
    group by geo_code, geography_key, legal_form, registered_month
    union all
    select
        geo_code,
        geography_key,
        '_T' as legal_form,
        registered_month,
        count(*) as entities
    from located
    where registered_month is not null
    group by geo_code, geography_key, registered_month
),

snapshot as (
    select
        max(requested_on) as as_of,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from entities
),

measured as (
    select
        'ms_krs:lei_selection:entities' as measure_key,
        'KRS entities loaded with a current extract (entities with an LEI; not a count of Polish companies)'
            as measure_name,
        'entities' as measure_codes,
        c.geo_code,
        c.geography_key,
        'D' as frequency,
        s.as_of as date_key,
        cast(s.as_of as varchar) as period_label,
        c.legal_form,
        c.entities
    from counted as c
    cross join snapshot as s
    union all
    select
        'ms_krs:lei_selection:registered' as measure_key,
        'KRS entities loaded, by month of registration in KRS (entities with an LEI; not all registrations)'
            as measure_name,
        'registered' as measure_codes,
        geo_code,
        geography_key,
        'M' as frequency,
        registered_month as date_key,
        strftime(registered_month, '%Y-%m') as period_label,
        legal_form,
        entities
    from registered
)

select
    'ms_krs' as source_system,
    'lei_selection' as block_id,
    'KRS entities loaded (selection: entities with an LEI)' as block_name,
    m.measure_key,
    m.measure_name,
    m.measure_codes,
    'entities' as unit_code,
    'legal entities' as unit_name,
    m.geo_code as geography_source_code,
    m.geography_key,
    m.frequency,
    m.date_key,
    m.period_label,
    cast(json_object('legal_form', m.legal_form) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    m.legal_form = '_T' as is_total,
    cast(m.entities as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from measured as m
cross join snapshot as s
