-- Gold build stage of the UNDP Human Development Report time series, by rule (docs/gold-coverage.md): every
-- row of silver.undp_hdr_indices with a value, every indicator and every country or aggregate, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py
-- to the table's files as one block. Relations:
--   silver    the data files of silver.undp_hdr_indices (area × indicator × year, newest report)
--   geo_map   gold.map_geography rows of UNDP HDR (ISO 3166 alpha-3 → country, ZZK.WORLD → WB:WLD)
--   measure_key = undp_hdr:undp_hdr_indices:_Z:<indicator>@A: an indicator (the file's column name without the
--                 year) is one measure whole, as WDI's codes are: hdi_f and le_m are the HDR's own indicators
--                 for a sex, gii_rank its own rank, so no series has a breakdown.
--   unit        = `as named`: the file has no unit column; the unit is the one the HDR technical notes give
--                 the indicator (reported as unmapped until a unit_map row names it, D33)
--   date_key    = 1 January of the year
--   geography   = by iso3 through map_geography; the HDR development groups and UNDP regions (ZZA … ZZJ) have no
--                 Gold area and are kept as _X with their code
--   status      = `ok`: the file has no status flags
-- A cell without a number has no value in Silver and is not loaded; nothing is 0-filled. A new report
-- recalculates the whole series, and Gold holds the newest report only.
select
    'undp_hdr' as source_system,
    'undp_hdr_indices' as block_id,
    'UNDP Human Development Report composite indices and components, complete time series' as block_name,
    'undp_hdr:undp_hdr_indices:_Z:' || s.indicator || '@A' as measure_key,
    'UNDP HDR ' || s.indicator as measure_name,
    s.indicator as measure_codes,
    'as named' as unit_code,
    'as named by the indicator' as unit_name,
    s.iso3 as geography_source_code,
    case when s.iso3 is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    'A' as frequency,
    make_date(s.year, 1, 1) as date_key,
    cast(s.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from silver as s
left join geo_map as g on s.iso3 = g.source_code
where s.value is not null and s.year between 1800 and 2200 and s.indicator is not null and s.indicator <> ''
