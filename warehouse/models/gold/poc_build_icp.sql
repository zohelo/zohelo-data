{{ config(alias='build_icp') }}
{# Gold PoC build stage: the World Bank ICP 2021 measures of the activated blocks (series × classification)
   in the common shape. Every economy and benchmark group ICP publishes (no EU aggregate); 2017 and 2021
   benchmarks, interim years where ICP publishes them. No breakdowns: every series is a total. #}
with blocks as ({{ poc_icp_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'world_bank_icp'
)

select
    'world_bank_icp' as source_system,
    o.series_code || ':' || o.classification_code as block_id,
    b.block_name,
    'world_bank_icp:' || o.series_code || ':' || o.classification_code as measure_key,
    b.block_name as measure_name,
    o.classification_code as measure_codes,
    o.classification_name as unit_code,
    o.classification_name as unit_name,
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
from {{ ref('icp_observations') }} as o
inner join blocks as b on o.series_code = b.series_code and o.classification_code = b.classification_code
left join geo_map as g on o.economy_code = g.source_code
where o.value is not null
