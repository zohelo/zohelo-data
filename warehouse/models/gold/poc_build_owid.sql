{{ config(alias='build_owid', tags=['gold_block']) }}
{# Gold build stage: the three Our World in Data charts of poc_owid_charts (register CD-031) from
   silver.owid_observations in the common shape, annual, one block and one measure per chart:
   measure_key owid:<chart>:<unit code> — consumption-based CO2 emissions per capita (Global Carbon Budget, tonnes
   per person), electoral democracy index (V-Dem, 0 to 1), GDP per capita (Maddison Project Database,
   international-$ in 2011 prices). Every country by ISO alpha-3, World and Kosovo (poc_map_geography section owid);
   OWID's continents, income groups and historical states have no Gold area and stay in Silver. Values as
   published, no status flags in the CSV (ok); source_version = the column's lastUpdated in metadata.json (else the
   Landing version). The charts publish no projections (tests/owid_observations_charts.sql fails on a year after
   the current one). Years before 1900 (Maddison from year 1, V-Dem from 1789) stay in Silver: the conformed
   calendar poc_dim_calendar starts on 1900-01-01 and the facts test date_key against it. #}
with charts as ({{ poc_owid_charts() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'owid'
),

meta as (
    select
        chart,
        short_name,
        last_updated
    from {{ ref('owid_charts') }}
)

select
    'owid' as source_system,
    o.chart as block_id,
    c.measure_name as block_name,
    'owid:' || o.chart || ':' || c.unit_code as measure_key,
    c.measure_name,
    o.indicator as measure_codes,
    c.unit_code,
    c.unit_name,
    o.entity_code as geography_source_code,
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
    coalesce(cast(m.last_updated as varchar), o.source_version) as source_version,
    o.raw_sha256
from {{ ref('owid_observations') }} as o
inner join charts as c on o.chart = c.chart and o.indicator = c.indicator
inner join geo_map as g on o.entity_code = g.source_code
left join meta as m on o.chart = m.chart and o.indicator = m.short_name
where
    o.value is not null
    and o.year >= 1900
