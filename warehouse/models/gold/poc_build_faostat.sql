{{ config(alias='build_faostat', tags=['gold_block']) }}
{# Gold build stage: FAOSTAT blocks of poc_faostat_blocks (emissions totals: GHG all sectors without LULUCF in kt
   CO2eq, AR5; land use: land area in 1000 ha) for the EU27 members, the FAO aggregate European Union (27)
   (→ EUROSTAT:EU27_2020) and World (→ WB:WLD), annual. measure_key faostat:<entity>:<unit>:<item>.<element>.<source>
   (the source code keeps FAO TIER 1 and other GT sources apart; '_Z' where the domain has none). Values and units
   as published; FAO flags (A official, E estimated, I imputed, ...) map to observation status ok. Areas through
   poc_map_geography (section faostat); unmapped areas are not built. #}
with blocks as ({{ poc_faostat_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'faostat' and geography_key <> '_X'
)

select
    'faostat' as source_system,
    o.entity || ':' || o.item_code || '.' || o.element_code as block_id,
    b.block_name,
    'faostat:' || o.entity || ':' || o.unit || ':' || o.item_code || '.' || o.element_code || '.' || o.source_code
        as measure_key,
    o.item || ', ' || o.element || coalesce(' (' || o.source || ')', '') as measure_name,
    o.item_code || '.' || o.element_code || '.' || o.source_code as measure_codes,
    o.unit as unit_code,
    o.unit as unit_name,
    cast(o.area_code as varchar) as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('faostat_observations') }} as o
inner join blocks as b on o.entity = b.entity and o.item_code = b.item_code and o.element_code = b.element_code
inner join geo_map as g on cast(o.area_code as varchar) = g.source_code
where o.value is not null
