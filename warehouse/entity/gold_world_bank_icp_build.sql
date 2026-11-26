-- Gold build stage of the World Bank International Comparison Program (ICP 2021, API source 90), by rule
-- (docs/gold-coverage.md): every row of silver.icp_observations with a value, every expenditure category,
-- classification, economy and benchmark group, in the common observation shape (the 21 columns of
-- poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one block.
-- Relations:
--   silver    the data files of silver.icp_observations (economy × classification × series × year, newest page of
--             every series)
--   geo_map   gold.map_geography rows of ICP (World Bank economy codes as WDI; ICP benchmark groups WDI does not
--             know are not listed and stay _X)
--   measure_key = world_bank_icp:icp_observations:<classification code>:<series code>@A: the classification is
--                 the measure and its unit at once (CN expenditure in local currency, PPPGlob PPP, PCAP.PP per
--                 capita in PPP international $, PX.WL price level index world = 100 …), the series the
--                 expenditure category (1000000 GDP, 9100000 actual individual consumption …), as the curated
--                 stage. No breakdowns: every series is a total.
--   unit        = the classification code (D33: unit_map.csv maps the stable code, as for the curated stage), its
--                 published classification name as the unit name
--   date_key    = 1 January of the year; a year outside 1800–2200 is not loaded
--   geography   = by economy code through map_geography; unmatched → _X, code kept
--   status      = `ok` for the benchmark years 2017 and 2021, which ICP surveys; `estimated` for every other year,
--                 which ICP 2021 fills between and around its benchmarks (silver.icp_observations: "benchmark
--                 years 2017 and 2021, interpolated years in between"), so they are never presented as surveyed
-- Not loaded: a cell without a number (no value in Silver; nothing is 0-filled, nor a value that is not a finite
-- number), and a year after the year of the series' version (lastupdated, the first 4-digit year in
-- source_version), which would be a projection.
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
        and s.classification_code is not null
        and s.series_code is not null
)

select
    'world_bank_icp' as source_system,
    'icp_observations' as block_id,
    'World Bank International Comparison Program 2021, every series and classification' as block_name,
    'world_bank_icp:icp_observations:' || p.classification_code || ':' || p.series_code || '@A' as measure_key,
    coalesce(p.series_name, p.series_code) || ', ' || coalesce(p.classification_name, p.classification_code)
        as measure_name,
    p.series_code as measure_codes,
    p.classification_code as unit_code,
    coalesce(p.classification_name, p.classification_code) as unit_name,
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
    case when p.year in (2017, 2021) then 'ok' else 'estimated' end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.economy_code = g.source_code
where p.date_key is not null and (p.version_year is null or p.year <= p.version_year)
