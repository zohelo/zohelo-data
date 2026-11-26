-- Gold build stage of the EEA greenhouse gas inventory (GovReg), by rule (docs/gold-coverage.md): every row of
-- silver.eea_ghg_inventory with a value, every reporting format, pollutant, sector and country, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to
-- the table's files as one block. Relations:
--   silver    the data files of silver.eea_ghg_inventory (country × format × pollutant × sector × year)
--   geo_map   gold.map_geography rows of EEA (ISO 3166-1 alpha-2 as published → country)
--   measure_key = eea:eea_ghg_inventory:<unit>:<format>:<pollutant>:<sector code>@A: the reporting format, the
--                 pollutant (or "All greenhouse gases - (CO2 equivalent)") and the CRF sector define the
--                 measure, names and codes as published ('|' becomes '/'); no breakdowns.
--   unit        = as published (Gg, Gg CO2 equivalent); D33, nothing is rescaled
--   date_key    = 1 January of the inventory year
--   geography   = by country code through map_geography; the EEA aggregate EUA has no Gold area and is kept as
--                 _X with its code
--   status      = `ok`; a number published together with a notation key `flagged`
-- Not loaded: a row with a notation key and no number (NO, NE, IE, NA, C …; Silver keeps it null, never 0),
-- and a 0 published together with a notation key (a notation key says there is no value).
select
    'eea' as source_system,
    'eea_ghg_inventory' as block_id,
    'EEA greenhouse gas inventory (GovReg), every format, pollutant, sector and country' as block_name,
    'eea:eea_ghg_inventory:' || replace(s.unit, '|', '/') || ':' || replace(s.format_name, '|', '/') || ':'
    || replace(s.pollutant_name, '|', '/') || ':' || replace(s.sector_code, '|', '/') || '@A' as measure_key,
    s.pollutant_name || ', ' || coalesce(s.sector_name, s.sector_code) || ' (' || s.format_name || ') [' || s.unit
    || ']' as measure_name,
    s.format_name || ':' || s.pollutant_name || ':' || s.sector_code as measure_codes,
    s.unit as unit_code,
    s.unit as unit_name,
    s.country_code as geography_source_code,
    case when s.country_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    'A' as frequency,
    make_date(s.year, 1, 1) as date_key,
    cast(s.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    case when s.notation is null then 'ok' else 'flagged' end as observation_status_key,
    s.source_version,
    s.raw_sha256
from silver as s
left join geo_map as g on s.country_code = g.source_code
where
    s.value is not null
    and not (s.value = 0 and s.notation is not null)
    and s.year between 1800 and 2200
