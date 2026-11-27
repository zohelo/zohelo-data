-- Gold build stage of the UNESCO UIS Data API selections, by rule (docs/gold-coverage.md): every row of
-- silver.unesco_uis_observations with a value, both selections and every geographic unit the UIS publishes them for,
-- in the common observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by
-- warehouse/observations.py to the table's files as one block. Relations:
--   silver    the data files of silver.unesco_uis_observations (indicator × geographic unit × year, newest copy)
--   geo_map   gold.map_geography rows of UNESCO UIS (countries by ISO 3166-1 alpha-3, poc_geography__unesco_uis)
--   measure_key = unesco_uis:unesco_uis_observations:<unit>:<base indicator>@A, as the curated stage: EXPGDP.TOT
--                 (GERD as % of GDP) and EA.5T8.AG25T99 (tertiary attainment, population 25+, %); the .F and .M
--                 variants of an indicator are its sex breakdown, {"sex": "T|F|M"} (T = the base indicator,
--                 is_total), never measures of their own
--   unit        = pct_gdp for GERD, pct for attainment (as published; D33). Any other indicator has no unit and so
--                 no measure_key: the block is rejected, never cut silently
--   date_key    = 1 January of the year; a year outside 1800–2200 is not loaded
--   geography   = by the published geo_unit through map_geography: countries by ISO alpha-3; the UIS, SDG, MDG,
--                 World Bank and UNESCO groupings ('SDG: World', 'WB: High income (July 2026)' …) stay _X with their
--                 code
--   status      = qualifier UIS_EST or NAT_EST (a UIS or national estimate) `estimated`, as the curated stage;
--                 magnitude NIL (nil or negligible, published as 0) `flagged`; any other qualifier or magnitude
--                 `flagged`; none `ok`
-- Not loaded: magnitude NA (not applicable; the API sends 0 in its value field, so it is not a value and never
-- becomes 0), a cell without a number (no value in Silver; nothing is 0-filled, nor a value that is not a finite
-- number), and a year after the year of the file's version, which would be a projection (the UIS publishes none).
with placed as (
    select
        s.*,
        regexp_replace(s.indicator_id, '\.(F|M)$', '') as base_indicator,
        case
            when s.indicator_id like '%.F' then 'F'
            when s.indicator_id like '%.M' then 'M'
            else 'T'
        end as sex_code,
        case when s.year between 1800 and 2200 then make_date(s.year, 1, 1) end as date_key,
        try_cast(nullif(regexp_extract(s.source_version, '(19|20)\d{2}'), '') as integer) as version_year
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        and coalesce(s.magnitude, '') <> 'NA'
),

units as (
    select
        p.*,
        case p.base_indicator when 'EXPGDP.TOT' then 'pct_gdp' when 'EA.5T8.AG25T99' then 'pct' end as unit_code
    from placed as p
)

select
    'unesco_uis' as source_system,
    'unesco_uis_observations' as block_id,
    'UNESCO UIS: GERD as % of GDP and tertiary attainment 25+, every geographic unit' as block_name,
    'unesco_uis:unesco_uis_observations:' || u.unit_code || ':' || u.base_indicator || '@A' as measure_key,
    case u.base_indicator
        when 'EXPGDP.TOT' then 'Gross domestic expenditure on R&D (GERD) as a percentage of GDP'
        else 'Educational attainment, at least completed short-cycle tertiary, population 25+ years, %'
    end as measure_name,
    u.base_indicator as measure_codes,
    u.unit_code,
    case u.unit_code when 'pct_gdp' then '% of GDP' else '%' end as unit_name,
    u.geo_unit as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    u.date_key,
    cast(u.year as varchar) as period_label,
    case when u.base_indicator = 'EXPGDP.TOT' then '{}' else '{"sex": "' || u.sex_code || '"}' end as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    case when u.base_indicator <> 'EXPGDP.TOT' then u.sex_code end as sex_code,
    u.sex_code = 'T' as is_total,
    u.value,
    case
        when u.qualifier in ('UIS_EST', 'NAT_EST') then 'estimated'
        when u.qualifier is not null or u.magnitude is not null then 'flagged'
        else 'ok'
    end as observation_status_key,
    u.source_version,
    u.raw_sha256
from units as u
left join geo_map as g on u.geo_unit = g.source_code
where u.date_key is not null and (u.version_year is null or u.year <= u.version_year)
