{{ config(alias='build_world_bank_gem', tags=['gold_block']) }}
{# Gold build stage: World Bank GEM series of poc_world_bank_gem_series for the EU27 members (ISO alpha-3 →
   poc_map_geography section world_bank_gem). Each series is built at one frequency only (monthly, real GDP
   quarterly): the annual and other rows GEM derives stay in Silver, so a series_key never holds two frequencies.
   measure_key world_bank_gem:<series>:<unit code>, breakdown {}. Values as published. #}
with series as ({{ poc_world_bank_gem_series() }}),

eu27 as ({{ poc_m49_eu27() }}),

geo_map as (
    select m.*
    from {{ ref('poc_map_geography') }} as m
    inner join eu27 as e on m.geography_key = e.iso2
    where m.source_system = 'world_bank_gem'
)

select
    'world_bank_gem' as source_system,
    o.entity as block_id,
    s.block_name,
    'world_bank_gem:' || o.series_code || ':' || s.unit_code as measure_key,
    o.series_name as measure_name,
    o.series_code as measure_codes,
    s.unit_code,
    s.unit_name,
    o.country_code as geography_source_code,
    g.geography_key,
    o.frequency,
    o.period_start as date_key,
    o.time_code as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('gem_observations') }} as o
inner join series as s on o.series_code = s.series_code and o.frequency = s.frequency
inner join geo_map as g on o.country_code = g.source_code
where o.value is not null
