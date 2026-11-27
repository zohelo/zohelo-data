-- Gold build stage of the Our World in Data charts (register CD-031), by rule (docs/gold-coverage.md): every row of
-- silver.owid_observations with a value, the three selected charts and every OWID entity (countries, OWID's own
-- areas, regions, income groups and historical states), in the common observation shape (the 21 columns of
-- poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one block. Relations:
--   silver    the data files of silver.owid_observations (chart × entity × year, newest copy of each chart)
--   geo_map   gold.map_geography rows of OWID (countries by ISO 3166-1 alpha-3, OWID_WRL World, OWID_KOS Kosovo;
--             poc_geography__owid)
--   charts    silver.owid_charts (chart, short_name, last_updated): the column's lastUpdated, the source's version
--   measure_key = owid:owid_observations:<unit>:<chart>@A: a chart has one value column, one measure (the chart's
--                 attribute columns, owid_region and the Maddison annotations, are not measures); no breakdowns
--   unit        = the curated stage's codes (poc_owid_charts; as published, D33): t_co2_per_person, index_0_1,
--                 intl_usd_2011_per_person. A chart not listed below has no unit and so no measure_key: the block is
--                 rejected, never cut silently
--   date_key    = 1 January of the year; period_label the year
--   geography   = the published code (ISO alpha-3 or OWID_…) through map_geography; an entity without a code (OWID's
--                 continents, Maddison regions, population-weighted aggregates, German and Italian states before
--                 unification) keeps its published name as the source code; whatever Gold has no area for (those,
--                 OWID's other OWID_… codes such as the USSR, Yugoslavia, the EU and income groups) stays _X with it
--   status      = `ok`: the chart CSV has no flags
-- Not loaded: a year outside 1800–2200 (the Maddison estimates for years 1 to 1799 and V-Dem's 1789-1799, as the
-- other rule stages: no area of the calendar holds them), an exact 0 in the consumption-based CO2 chart (zero
-- consumption emissions for a populated area is a missing value, not an observation; the curated stage's rule, one
-- cell: Namibia 1990), a cell without a number (no Silver row; nothing is 0-filled, nor a value that is not a finite
-- number) and a year after the year of the column's lastUpdated (a projection; the charts publish none).
with chart_units (chart, unit_code, unit_name, measure_name) as (
    values
    (
        'consumption_co2_per_capita', 't_co2_per_person', 'tonnes of CO2 per person',
        'Consumption-based CO2 emissions per capita (Global Carbon Budget, via Our World in Data)'
    ),
    (
        'electoral_democracy_index', 'index_0_1', 'index (0 to 1)',
        'Electoral democracy index, best estimate (V-Dem, via Our World in Data)'
    ),
    (
        'gdp_per_capita_maddison', 'intl_usd_2011_per_person', 'international-$ in 2011 prices per person',
        'GDP per capita (Maddison Project Database, via Our World in Data)'
    )
),

placed as (
    select
        s.*,
        coalesce(s.entity_code, s.entity_name) as area_code,
        c.last_updated,
        case when s.year between 1800 and 2200 then make_date(s.year, 1, 1) end as date_key,
        year(coalesce(c.last_updated, try_strptime(left(s.source_version, 8), '%Y%m%d'))) as version_year
    from silver as s
    left join charts as c on s.chart = c.chart and s.indicator = c.short_name
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        and not (s.chart = 'consumption_co2_per_capita' and s.value = 0)
)

select
    'owid' as source_system,
    'owid_observations' as block_id,
    'Our World in Data charts (Global Carbon Budget, V-Dem, Maddison), every entity' as block_name,
    'owid:owid_observations:' || u.unit_code || ':' || p.chart || '@A' as measure_key,
    u.measure_name,
    p.indicator as measure_codes,
    u.unit_code,
    u.unit_name,
    p.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    p.date_key,
    cast(p.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.value,
    'ok' as observation_status_key,
    coalesce(cast(p.last_updated as varchar), p.source_version) as source_version,
    p.raw_sha256
from placed as p
left join chart_units as u on p.chart = u.chart
left join geo_map as g on p.area_code = g.source_code
where p.date_key is not null and (p.version_year is null or p.year <= p.version_year)
