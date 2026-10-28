{{ config(alias='build_wdi') }}
{# Gold PoC build stage: the World Bank WDI indicators of the PoC concepts in the common shape.
   No breakdowns: every series is a total. #}
with blocks as ({{ poc_wdi_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'world_bank_wdi'
),

indicators as (select * from {{ ref('wdi_indicators') }})

select
    'world_bank_wdi' as source_system,
    o.indicator_code as block_id,
    coalesce(i.indicator_name, o.indicator_code) as block_name,
    'world_bank_wdi:' || o.indicator_code as measure_key,
    coalesce(i.indicator_name, o.indicator_code) as measure_name,
    '' as measure_codes,
    coalesce(nullif(i.unit_of_measure, ''), 'as named') as unit_code,
    coalesce(nullif(i.unit_of_measure, ''), 'as named') as unit_name,
    o.economy_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
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
from {{ ref('wdi_observations') }} as o
inner join blocks as b on o.indicator_code = b.indicator_code
left join indicators as i on o.indicator_code = i.indicator_code
left join geo_map as g on o.economy_code = g.source_code
where o.value is not null
