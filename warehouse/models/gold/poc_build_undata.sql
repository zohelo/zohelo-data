{{ config(alias='build_undata', tags=['gold_block']) }}
{# Gold build stage: the UN Statistical Yearbook series of poc_undata_series from silver.undata_syb_values (newest
   edition of each table) in the common shape, annual (the Yearbook prints selected years), for the EU27 members
   (UN M49 → poc_map_geography section undata, static; macro poc_m49_eu27) and World (M49 1 → WB:WLD); other
   regions are not built. measure_key undata:<table>:<unit as in the series name>:<measure code>; measure_name
   is the series as published. Values as published (millions of US dollars stay millions, D33); an entry
   without a number is not loaded. Footnotes are free text: a row without one is ok; "Estimate…" →
   estimated, a break or provisional note → break / provisional, and any other footnote (the population covered,
   the accounting standard, the reference period: "Population aged 16 to 74 years.") → definition_differs, so no
   footnoted value passes as a plain one; the text stays in silver.undata_syb_values. A row footnoted as a
   projection is not an observation and is left out (none in the built series today).
   tests/undata_gold_series_present.sql fails when a series of poc_undata_series matches no Silver row (a new
   edition that renames a series). #}
with series as ({{ poc_undata_series() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'undata' and geography_key <> '_X'
)

select
    'undata' as source_system,
    o.entity as block_id,
    s.block_name,
    'undata:' || o.entity || ':' || s.unit_code || ':' || s.measure_code as measure_key,
    o.series as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    s.unit_code as unit_name,
    o.area_code as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    case
        when o.footnotes is null then 'ok'
        when o.footnotes ilike '%estimate%' then 'estimated'
        when o.footnotes ilike '%break%' then 'break'
        when o.footnotes ilike '%provisional%' or o.footnotes ilike '%preliminary%' then 'provisional'
        else 'definition_differs'
    end as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('undata_syb_values') }} as o
inner join series as s on o.entity = s.entity and o.series = s.series
inner join geo_map as g on o.area_code = g.source_code
where o.value is not null and o.year is not null and coalesce(o.footnotes, '') not ilike '%project%'
