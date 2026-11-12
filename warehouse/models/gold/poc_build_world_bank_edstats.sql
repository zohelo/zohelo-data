{{ config(alias='build_world_bank_edstats', tags=['gold_block']) }}
{# Gold build stage: OECD PISA mean scores (World Bank EdStats republication, blocks of poc_world_bank_edstats_blocks)
   for the EU27 members (ISO alpha-3 → poc_map_geography section world_bank_edstats), by assessment year (annual
   frequency, one value every three years). One measure per subject with a sex breakdown: the base indicator = both
   sexes (sex T, is_total), .FE = F, .MA = M, breakdown_json {"sex": ...}. measure_key
   world_bank_edstats:<entity>:score:<base indicator>. Scores as published (PISA scale points). The source sends
   no status flag on these indicators (obs_status empty); a flag fails the build until it is mapped. #}
with blocks as ({{ poc_world_bank_edstats_blocks() }}),

eu27 as ({{ poc_m49_eu27() }}),

geo_map as (
    select m.*
    from {{ ref('poc_map_geography') }} as m
    inner join eu27 as e on m.geography_key = e.iso2
    where m.source_system = 'world_bank_edstats'
)

select
    'world_bank_edstats' as source_system,
    o.entity as block_id,
    b.block_name,
    'world_bank_edstats:' || o.entity || ':score:' || b.indicator_id as measure_key,
    b.block_name as measure_name,
    b.indicator_id as measure_codes,
    'score' as unit_code,
    'PISA score points' as unit_name,
    o.country_iso3 as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{"sex": "' || o.sex_code || '"}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    o.sex_code,
    o.sex_code = 'T' as is_total,
    o.value,
    case
        when o.obs_status is null then 'ok'
        else error('build_world_bank_edstats: unmapped obs_status ' || o.obs_status)
    end as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('edstats_pisa') }} as o
inner join blocks as b on o.entity = b.entity
inner join geo_map as g on o.country_iso3 = g.source_code
where o.value is not null and o.indicator_id in (b.indicator_id, b.indicator_id || '.FE', b.indicator_id || '.MA')
