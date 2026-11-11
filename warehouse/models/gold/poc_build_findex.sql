{{ config(alias='build_findex') }}
{# Gold PoC build stage: the World Bank Global Findex indicators of the activated blocks in the common shape,
   whole adult population (group all) only. Findex publishes shares 0-1; Gold holds per cent (× 100), like the
   WDI copy of the same indicator. Survey years 2011, 2014, 2017, 2021, 2022, 2024 (not every economy in
   every wave). Aggregates are World Bank regions and income groups; no EU aggregate. #}
with blocks as ({{ poc_findex_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'world_bank_findex'
)

select
    'world_bank_findex' as source_system,
    o.indicator as block_id,
    b.block_name,
    'world_bank_findex:' || o.indicator as measure_key,
    b.block_name as measure_name,
    '' as measure_codes,
    '% of population age 15+' as unit_code,
    '% of population age 15+' as unit_name,
    o.economy_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value * 100 as value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('findex_observations') }} as o
inner join blocks as b on o.indicator = b.indicator
left join geo_map as g on o.economy_code = g.source_code
where o.group_name = 'all' and o.group_value = 'all' and o.value is not null
