{{ config(alias='build_wgi', tags=['gold_block']) }}
{# Gold PoC build stage: the World Bank WGI governance estimates of the activated indicators in the common
   shape. Every economy WGI publishes (countries and territories; WGI has no aggregates, so no EU value).
   No breakdowns: every series is a total. #}
with blocks as ({{ poc_wgi_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'world_bank_wgi'
),

indicators as (select * from {{ ref('wgi_indicators') }})

select
    'world_bank_wgi' as source_system,
    o.indicator_code as block_id,
    coalesce(i.indicator_name, o.indicator_code) as block_name,
    'world_bank_wgi:' || o.indicator_code as measure_key,
    coalesce(i.indicator_name, o.indicator_code) as measure_name,
    '' as measure_codes,
    'governance estimate (about -2.5 to 2.5)' as unit_code,
    'governance estimate (about -2.5 to 2.5)' as unit_name,
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
from {{ ref('wgi_observations') }} as o
inner join blocks as b on o.indicator_code = b.indicator_code
left join indicators as i on o.indicator_code = i.indicator_code
left join geo_map as g on o.economy_code = g.source_code
where o.value is not null
