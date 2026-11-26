-- Gold build stage of the FAOSTAT bulk domains, by rule (docs/gold-coverage.md): every row of
-- silver.faostat_observations with a value, every domain, item, element, source and area, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to
-- the table's files as one block. Relations:
--   silver    the data files of silver.faostat_observations (domain × area × item × element × year × source)
--   geo_map   gold.map_geography rows of FAOSTAT (FAO area code → the EU27 members, 5707 → EU27_2020, 5000 → World)
--   measure_key = faostat:faostat_observations:<unit>:<domain>:<item code>.<element code>.<source code>@A: the
--                 domain (Silver entity: emissions_totals, land_use), item (sector, land category), element
--                 (gas, emissions or area kind) and FAO's data source (GT: FAO TIER 1 and others; `_Z` where the
--                 domain has none) define the measure, as in the curated stage; two FAO sources of one item are
--                 two measures, never one chosen value. No breakdowns.
--   unit        = FAO's unit as published (kt, 1000 ha, %); D33, nothing is rescaled
--   date_key    = 1 January of the year (both domains are annual)
--   geography   = by FAO area code through map_geography; every other FAO area code (countries outside the EU27,
--                 FAO regions and groups) is kept as _X with its code
--   status      = FAO flag: E and I estimated, B break, P provisional, A (official), X (international
--                 organisation) and none ok, any other flag `flagged` (never silently ok)
-- Not loaded: a cell without a value (nothing is 0-filled); a cell flagged M (FAO: missing value, data cannot
-- exist), whatever number it carries; a projection (Silver is_projection: a year after the year of the file's
-- version; GT carries projected years to 2050).
select
    'faostat' as source_system,
    'faostat_observations' as block_id,
    'FAOSTAT bulk domains (emissions totals, land use), every area' as block_name,
    'faostat:faostat_observations:' || s.unit || ':' || s.entity || ':'
    || s.item_code || '.' || s.element_code || '.' || s.source_code || '@A' as measure_key,
    s.item || ', ' || s.element || coalesce(' (' || s.source || ')', '') || ' [' || s.unit || ']' as measure_name,
    s.entity || ':' || s.item_code || '.' || s.element_code || '.' || s.source_code as measure_codes,
    s.unit as unit_code,
    s.unit as unit_name,
    cast(s.area_code as varchar) as geography_source_code,
    case when s.area_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    'A' as frequency,
    make_date(s.year, 1, 1) as date_key,
    cast(s.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    case coalesce(s.flag, 'A')
        when 'A' then 'ok' when 'X' then 'ok'
        when 'E' then 'estimated' when 'I' then 'estimated'
        when 'B' then 'break' when 'P' then 'provisional'
        else 'flagged'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from silver as s
left join geo_map as g on cast(s.area_code as varchar) = g.source_code
where
    s.value is not null
    and coalesce(s.flag, '') <> 'M'
    and not s.is_projection
    and s.year between 1800 and 2200
