{{ config(alias='build_eea', tags=['gold_block']) }}
{# Gold build stage: EEA greenhouse gas inventory (GovReg, block govreg_ghg_inventory of poc_eea_blocks): all
   greenhouse gases in CO2 equivalent (pollutant "All greenhouse gases - (CO2 equivalent)", unit Gg CO2 equivalent
   = kt) for the totals with and without LULUCF and the CRF main sectors listed in poc_eea_sectors, the EU27 members
   (ISO alpha-2 → poc_map_geography section eea), annual. measure_key eea:govreg_ghg_inventory:gg_co2e:<sector
   code as published>, breakdown {}. Rows with a notation key and no value (NO, NE, IE, NA, C, ...) are not loaded;
   values as published (LULUCF can be negative). The EEA aggregate EUA is not mapped (its scope is not checked). #}
with sectors as ({{ poc_eea_sectors() }}),

eu27 as ({{ poc_m49_eu27() }}),

geo_map as (
    select m.*
    from {{ ref('poc_map_geography') }} as m
    inner join eu27 as e on m.geography_key = e.iso2
    where m.source_system = 'eea'
)

select
    'eea' as source_system,
    o.entity as block_id,
    'Greenhouse gas emissions by sector, GovReg inventory (EEA)' as block_name,
    'eea:' || o.entity || ':gg_co2e:' || o.sector_code as measure_key,
    'Greenhouse gas emissions (CO2 equivalent): ' || o.sector_name as measure_name,
    o.sector_code as measure_codes,
    o.unit as unit_code,
    o.unit as unit_name,
    o.country_code as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    s.is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('eea_ghg_inventory') }} as o
inner join sectors as s on o.sector_code = s.sector_code
inner join geo_map as g on o.country_code = g.source_code
where
    o.value is not null
    and o.pollutant_name = 'All greenhouse gases - (CO2 equivalent)'
    and o.unit = 'Gg CO2 equivalent'
