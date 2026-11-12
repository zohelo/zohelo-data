{{ config(alias='build_unhcr', tags=['gold_block']) }}
{# Gold build stage: UNHCR Refugee Statistics blocks of poc_unhcr_blocks, end-of-year stocks in persons, annual.
   population_by_asylum: every EU27 member as country of asylum (ISO alpha-3 → poc_map_geography section unhcr),
   origins summed by UNHCR, breakdown {}; poland_by_origin: Poland by country of origin, breakdown
   {"origin": <ISO alpha-3 as published>} (origin "-" never occurs, tests/unhcr_selection.sql). measure_key
   unhcr:<block>:persons:<population type>. A null ("-" in the source: no figure) is not loaded; values as
   published. #}
with blocks as ({{ poc_unhcr_blocks() }}),

eu27 as ({{ poc_m49_eu27() }}),

geo_map as (
    select m.*
    from {{ ref('poc_map_geography') }} as m
    inner join eu27 as e on m.geography_key = e.iso2
    where m.source_system = 'unhcr'
),

long as (
    select
        o.entity,
        o.year,
        o.coa_iso,
        o.coo_iso,
        o.coo_name,
        u.population_type,
        u.value,
        o.source_version,
        o.raw_sha256
    from {{ ref('unhcr_population') }} as o,
        unnest(
            [
                {'population_type': 'refugees', 'value': o.refugees},
                {'population_type': 'asylum_seekers', 'value': o.asylum_seekers},
                {'population_type': 'stateless', 'value': o.stateless},
                {'population_type': 'oip', 'value': o.oip}
            ]
        ) as t (u)
)

select
    'unhcr' as source_system,
    l.entity as block_id,
    b.block_name,
    'unhcr:' || l.entity || ':persons:' || l.population_type as measure_key,
    b.block_name || ': ' || l.population_type as measure_name,
    l.population_type as measure_codes,
    'persons' as unit_code,
    'persons' as unit_name,
    l.coa_iso as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(l.year, 1, 1) as date_key,
    cast(l.year as varchar) as period_label,
    case when l.entity = 'poland_by_origin' then '{"origin": "' || l.coo_iso || '"}' else '{}' end as breakdown_json,
    case when l.entity = 'poland_by_origin' then cast(to_json(map(['origin'], [l.coo_name])) as varchar) end
        as breakdown_label_json,
    cast(null as varchar) as sex_code,
    l.entity = 'population_by_asylum' as is_total,
    cast(l.value as double) as value,
    'ok' as observation_status_key,
    l.source_version,
    l.raw_sha256
from long as l
inner join blocks as b on l.entity = b.entity
inner join geo_map as g on l.coa_iso = g.source_code
where l.value is not null
