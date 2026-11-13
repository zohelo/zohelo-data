-- Gold build stage of World Bank WDI, by rule (docs/gold-coverage.md): every row of silver.wdi_observations
-- with a value, every indicator and every economy, in the common observation shape (the 21 columns of
-- poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one block.
-- Relations:
--   silver       the data files of silver.wdi_observations (economy × indicator × year)
--   geo_map      gold.map_geography rows of WDI (source_code, geography_key)
--   indicators   silver.wdi_indicators (name, unit of measure)
--   measure_key = world_bank_wdi:wdi_observations:_Z:<indicator code>@A. The unit is not part of the key:
--                 WDI states it in the indicator's name and in a metadata field that is filled or corrected
--                 without the indicator changing, and a key must not change with it. An indicator code is
--                 one measure whole (codes ending .FE, .MA or an age band are WDI's own indicators, not
--                 breakdowns of another), so no series has a breakdown.
--   unit        = the metadata field as published, else `as named` (the unit_map.csv code of WDI, D33)
--   date_key    = 1 January of the year; a year outside 1800–2200 is not loaded
--   geography   = by economy code through map_geography; unmatched → _X, code kept
--   status      = a year after the year of the archive's version cannot be an observation: `forecast`
-- A cell without a number (empty in the file) has no value in Silver and is not loaded; nothing is 0-filled.
with placed as (
    select
        s.*,
        case when s.year between 1800 and 2200 then make_date(s.year, 1, 1) end as date_key,
        try_cast(substr(s.source_version, 1, 4) as integer) as version_year
    from silver as s
    where s.value is not null
),

names as (
    select
        indicator_code,
        any_value(indicator_name) as indicator_name,
        any_value(nullif(trim(unit_of_measure), '')) as unit_of_measure,
        count(*) as rows_
    from indicators
    group by indicator_code
)

select
    'world_bank_wdi' as source_system,
    'wdi_observations' as block_id,
    'World Development Indicators' as block_name,
    'world_bank_wdi:wdi_observations:_Z:' || p.indicator_code || '@A' as measure_key,
    coalesce(n.indicator_name, p.indicator_code) as measure_name,
    p.indicator_code as measure_codes,
    coalesce(n.unit_of_measure, 'as named') as unit_code,
    coalesce(n.unit_of_measure, 'as named by the indicator') as unit_name,
    p.economy_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    p.date_key,
    cast(p.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.value,
    case when p.year > p.version_year then 'forecast' else 'ok' end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
-- silver.wdi_indicators is unique by indicator_code (its dbt test); a repeated code must not repeat rows
left join names as n on p.indicator_code = n.indicator_code and n.rows_ = 1
left join geo_map as g on p.economy_code = g.source_code
where p.date_key is not null
