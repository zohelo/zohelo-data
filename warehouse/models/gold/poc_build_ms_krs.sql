{{ config(alias='build_ms_krs', tags=['gold_block']) }}
{# Gold build stage: Open KRS API, an entity snapshot of what is loaded (silver.ms_krs_entities with a current
   extract; one date, the day of the newest request). Measure ms_krs:lei_selection:entities = entities loaded,
   by legal form (breakdown legal_form as the register prints it, _T all, _U not stated) and seat voivodeship
   (the register's name → poc_map_geography section ms_krs; PL = all entities loaded, wherever the seat). The
   selection is the KRS numbers GLEIF lists, loaded over several runs: this is a count of loaded entities, not
   of Polish companies, and registrations per year are left out for the same reason. An extract without a
   voivodeship adds no voivodeship row; names that are no voivodeship are counted together under _X. #}
with entities as (
    select
        coalesce(legal_form, '_U') as legal_form,
        seat_voivodeship,
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
        legal_form
    from entities
    union all
    select
        coalesce(g.source_code, '_X') as geo_code,
        coalesce(g.geography_key, '_X') as geography_key,
        e.legal_form
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

snapshot as (
    select
        max(requested_on) as as_of,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from entities
)

select
    'ms_krs' as source_system,
    'lei_selection' as block_id,
    'KRS entities loaded (selection: entities with an LEI)' as block_name,
    'ms_krs:lei_selection:entities' as measure_key,
    'KRS entities loaded with a current extract (entities with an LEI; not a count of Polish companies)'
        as measure_name,
    'entities' as measure_codes,
    'entities' as unit_code,
    'legal entities' as unit_name,
    c.geo_code as geography_source_code,
    c.geography_key,
    'D' as frequency,
    s.as_of as date_key,
    cast(s.as_of as varchar) as period_label,
    cast(json_object('legal_form', c.legal_form) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.legal_form = '_T' as is_total,
    cast(c.entities as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from counted as c
cross join snapshot as s
