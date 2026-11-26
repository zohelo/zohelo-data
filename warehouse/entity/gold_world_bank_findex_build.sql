-- Gold build stage of the World Bank Global Findex Database, by rule (docs/gold-coverage.md): every row of
-- silver.findex_observations with a value, every indicator, economy, survey year and population group, in the
-- common observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by
-- warehouse/observations.py to the table's files as one block. Relations:
--   silver    the data files of silver.findex_observations (economy × survey year × group × indicator, newest
--             edition)
--   geo_map   gold.map_geography rows of Findex (World Bank economy codes: ISO3 countries, WB regions and income
--             groups, as WDI)
--   measure_key = world_bank_findex:findex_observations:_Z:<indicator>@A: an indicator code is one measure whole
--                 (account_t_d, borrow_any_t_d …). The unit is not in the key: Findex states it nowhere per row.
--   breakdown   = the population group as published: {"group": "<group>", "group_value": "<value>"} (group all =
--                 the whole adult population, is_total); a share for women and one for men are two series of
--                 one measure, never one chosen value.
--   unit        = a share of the population group, 0-1, as published (D33): the curated stage's unit code
--                 "share of population age 15+" holds for group all only, so the rule names the group instead.
--                 Nothing is rescaled; per cent belongs to the KPI layer.
--   date_key    = 1 January of the survey year (2011, 2014, 2017, 2021, 2022, 2024; not every economy in every
--                 wave); a year outside 1800–2200 is not loaded
--   geography   = by economy code through map_geography; codes Gold has no area for stay _X with their code
--   status      = `ok`: the file has no flags per cell
-- Not loaded: a cell without a number (empty or `NA` in the file, so no value in Silver; nothing is 0-filled,
-- and a value that is not a finite number), and a year after the year of the edition's version (the first
-- 4-digit year in source_version): Findex publishes survey results only, so such a row would not be an
-- observation.
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
        and s.indicator is not null
)

select
    'world_bank_findex' as source_system,
    'findex_observations' as block_id,
    'World Bank Global Findex Database, every indicator, economy and population group' as block_name,
    'world_bank_findex:findex_observations:_Z:' || p.indicator || '@A' as measure_key,
    p.indicator as measure_name,
    p.indicator as measure_codes,
    'share of population group' as unit_code,
    'share of the population group (0-1), as published' as unit_name,
    p.economy_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    p.date_key,
    cast(p.year as varchar) as period_label,
    '{"group": ' || to_json(p.group_name) || ', "group_value": ' || to_json(p.group_value) || '}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    p.group_name = 'all' and p.group_value = 'all' as is_total,
    p.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.economy_code = g.source_code
where p.date_key is not null and (p.version_year is null or p.year <= p.version_year)
