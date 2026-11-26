-- Gold build stage of the World Bank Worldwide Governance Indicators, by rule (docs/gold-coverage.md): every row
-- of silver.wgi_observations with a value, every indicator and economy, in the common observation shape (the 21
-- columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one
-- block. Relations:
--   silver       the data files of silver.wgi_observations (economy × indicator × year, newest archive)
--   geo_map      gold.map_geography rows of WGI (World Bank economy codes as WDI)
--   indicators   silver.wgi_indicators (name, unit of measure)
--   measure_key = world_bank_wgi:wgi_observations:_Z:<indicator code>@A: an indicator code is one measure whole
--                 (GOV_WGI_<dimension>.<EST|SC|SC_LB|SC_UB|SE|SR>: the estimate, the 0-100 score with its 90 %
--                 bounds, the standard error and the number of sources of six governance dimensions), as for WDI.
--                 The unit is not in the key: the metadata field can be filled or corrected without the
--                 indicator changing. No breakdowns: WGI publishes countries and territories only.
--   unit        = the metadata field as published, else `as named` (D33; the curated stage's code for the
--                 estimates stays in build_wgi)
--   date_key    = 1 January of the year (1996, 1998, 2000, then yearly); a year outside 1800–2200 is not loaded
--   geography   = by economy code through map_geography; unmatched → _X, code kept
--   status      = `ok`: WGI publishes no status per cell
-- Not loaded: a cell without a number (`..` or empty in the file, so no value in Silver; nothing is 0-filled, nor
-- a value that is not a finite number), and a year after the year of the archive's version (the first 4-digit
-- year in source_version), which would not be an observation.
with placed as (
    select
        s.*,
        case when s.year between 1800 and 2200 then make_date(s.year, 1, 1) end as date_key,
        try_cast(nullif(regexp_extract(s.source_version, '(19|20)\d{2}'), '') as integer) as version_year
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        and s.indicator_code is not null
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
    'world_bank_wgi' as source_system,
    'wgi_observations' as block_id,
    'Worldwide Governance Indicators, every indicator and economy' as block_name,
    'world_bank_wgi:wgi_observations:_Z:' || p.indicator_code || '@A' as measure_key,
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
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
-- silver.wgi_indicators is unique by indicator_code (its dbt test); a repeated code must not repeat rows
left join names as n on p.indicator_code = n.indicator_code and n.rows_ = 1
left join geo_map as g on p.economy_code = g.source_code
where p.date_key is not null and (p.version_year is null or p.year <= p.version_year)
