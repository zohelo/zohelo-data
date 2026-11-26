-- Gold build stage of the World Bank International Debt Statistics, by rule (docs/gold-coverage.md): every row
-- of silver.ids_observations with a value, every series, debtor economy and counterpart area, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to
-- the table's files as one block. Relations:
--   silver    the data files of silver.ids_observations (debtor × counterpart × series × year, newest archive)
--   geo_map   gold.map_geography rows of World Bank IDS (World Bank economy codes: ISO3 countries, WB groups)
--   measure_key = world_bank_ids:ids_observations:_Z:<series code>@A: a series code is one measure whole, as for
--                 WDI. The unit is not in the key (`_Z`): IDS states it in the series name only.
--   breakdown   = the counterpart area (creditor) as published: {"counterpart": "<code>"} (WLD = all creditors,
--                 is_total); a debtor's debt to two creditors is two series, never one chosen value.
--   unit        = `as named` (the series name carries it: current US$, % of GNI, % of exports …)
--   date_key    = 1 January of the year
--   geography   = the debtor economy through map_geography; codes Gold has no area for stay _X with their code
--   status      = `ok`: the archive has no flags
-- Not loaded: a cell without a number (nothing is 0-filled), and a projection: IDS publishes scheduled debt
-- service up to eight years ahead, so a year after the last actual year (the newest year with a value of total
-- external debt stocks, DT.DOD.DECT.CD, as in the curated stage; the year of the archive's version when that
-- series is missing) is not an observation.
with last_actual as (
    select
        coalesce(
            max(s.year) filter (where s.series_code = 'DT.DOD.DECT.CD' and s.value is not null),
            max(try_cast(regexp_extract(s.source_version, '(\d{4})', 1) as integer))
        ) as year
    from silver as s
)

select
    'world_bank_ids' as source_system,
    'ids_observations' as block_id,
    'World Bank International Debt Statistics, every series, debtor and counterpart' as block_name,
    'world_bank_ids:ids_observations:_Z:' || s.series_code || '@A' as measure_key,
    coalesce(s.series_name, s.series_code) as measure_name,
    s.series_code as measure_codes,
    'as named' as unit_code,
    'as named by the series' as unit_name,
    s.economy_code as geography_source_code,
    case when s.economy_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    'A' as frequency,
    make_date(s.year, 1, 1) as date_key,
    cast(s.year as varchar) as period_label,
    '{"counterpart": ' || to_json(coalesce(s.counterpart_code, '_Z')) || '}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    coalesce(s.counterpart_code, 'WLD') = 'WLD' as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from silver as s
cross join last_actual as la
left join geo_map as g on s.economy_code = g.source_code
where
    s.value is not null
    and not isnan(s.value)
    and s.series_code is not null
    and s.year between 1800 and 2200
    and s.year <= la.year
