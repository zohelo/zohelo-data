{# Our World in Data chart values (register CD-031; source bronze.owid_values, the newest copy of each chart CSV
   of https://ourworldindata.org/grapher/<slug>.csv): one row per chart, OWID entity and year with a value, typed.
   Grain: chart × entity_name × year (entity_name, because OWID's regions and income groups have no code).
   Charts and their value columns: poc_owid_charts() (macros/sources/owid.sql). entity_code is the published code
   as is (ISO 3166-1 alpha-3, OWID_… for OWID's own areas, null for regions and groups); owid_region (V-Dem chart)
   and annotation (Maddison chart) are the chart's attribute columns of the same CSV row. A cell that is empty in
   the CSV has no Bronze row and so no Silver row (never a zero); a value that is not a number fails the cast,
   and the build with it. Whole series are revised in place, so each new copy replaces the chart. #}
with charts as ({{ poc_owid_charts() }}),

newest as (
    select *
    from {{ source('owid', 'owid_values') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
),

attributes as (
    select
        raw_sha256,
        source_row_number,
        max(case when column_name = 'owid_region' then value end) as owid_region,
        max(case when column_name = 'gdp_per_capita__annotations' then value end) as annotation
    from newest
    where column_name in ('owid_region', 'gdp_per_capita__annotations')
    group by raw_sha256, source_row_number
)

select
    n.entity as chart,
    n.entity_name,
    nullif(n.code, '') as entity_code,
    cast(n.year as integer) as year,
    n.column_name as indicator,
    cast(n.value as double) as value,
    a.owid_region,
    a.annotation,
    n.raw_sha256,
    n.source_version,
    n.ingested_at
from newest as n
inner join charts as c on n.entity = c.chart and n.column_name = c.indicator
left join attributes as a on n.raw_sha256 = a.raw_sha256 and n.source_row_number = a.source_row_number
