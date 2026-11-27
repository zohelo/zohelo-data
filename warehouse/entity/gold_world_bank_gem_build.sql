-- Gold build stage of the World Bank Global Economic Monitor (API source 15), by rule (docs/gold-coverage.md): every
-- row of silver.gem_observations with a value, every declared series, economy and frequency GEM publishes it at, in
-- the common observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by
-- warehouse/observations.py to the table's files as one block. Relations:
--   silver    the data files of silver.gem_observations (series × economy × period, newest copy; Silver holds the
--             EU27 members, as the landed requests ask)
--   geo_map   gold.map_geography rows of GEM (countries by ISO 3166-1 alpha-3, poc_geography__world_bank_gem)
--   measure_key = world_bank_gem:gem_observations:<unit>:<series code>@<A|Q|M>: a series code is one measure whole
--                 (GEM's codes carry the adjustment: CPTOTSAXN, CPTOTSAXNZGY …); one answer mixes annual, quarterly
--                 and monthly periods of the same series, so each frequency is a measure of its own and a
--                 series_key never holds two frequencies. The curated stage keeps one frequency per series.
--   unit        = as GEM names it per series, the codes of the curated stage (poc_world_bank_gem_series; D33,
--                 nothing is rescaled): pct (% y-o-y, unemployment %), index, constant_usd, lcu_per_usd,
--                 mln_constant_2010_usd. A series not listed below has no unit and so no measure_key: the block is
--                 rejected, never cut silently
--   date_key    = the first day of the period (Silver period_start); period_label the published code (2025, 2025Q3,
--                 2025M07)
--   geography   = by ISO alpha-3 through map_geography; a code Gold has no country for stays _X with its code
--   status      = `ok`: the API sends no flags for these series
-- Not loaded: a period that had not ended on the day of the file's version (source_version), an incomplete period
-- or a projection, never an observation (GEM's annual 2026 rows in the September 2026 copy: an average or a sum of
-- the months and quarters so far); a cell without a number (no value in Silver; nothing is 0-filled, nor a value
-- that is not a finite number). A 0 is a published value (a y-o-y change of 0.0%).
with series (series_code, unit_code, unit_name, series_name) as (
    values
    ('CPTOTSAXNZGY', 'pct', '% y-o-y', 'CPI price, % y-o-y, seasonally adjusted'),
    ('CPTOTSAXN', 'index', 'index', 'CPI price index, seasonally adjusted'),
    ('CORESA', 'index', 'index', 'Core CPI index, seasonally adjusted'),
    ('IPTOTSAKD', 'constant_usd', 'constant US$', 'Industrial production, constant US$, seasonally adjusted'),
    ('UNEMPSA_', 'pct', '%', 'Unemployment rate, seasonally adjusted'),
    ('RETSALESSA', 'index', 'index', 'Retail sales volume index, seasonally adjusted'),
    ('DPANUSSPB', 'lcu_per_usd', 'LCU per US$', 'Exchange rate, LCU per US$, period average'),
    (
        'NYGDPMKTPSAKD', 'mln_constant_2010_usd', 'constant 2010 US$, millions',
        'GDP, constant 2010 US$, millions, seasonally adjusted'
    )
),

placed as (
    select
        s.*,
        case s.frequency
            when 'A' then s.period_start + interval 1 year
            when 'Q' then s.period_start + interval 3 month
            when 'M' then s.period_start + interval 1 month
        end as period_end_next,
        try_strptime(left(s.source_version, 8), '%Y%m%d') as version_day
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
)

select
    'world_bank_gem' as source_system,
    'gem_observations' as block_id,
    'World Bank Global Economic Monitor, every landed series, economy and frequency' as block_name,
    'world_bank_gem:gem_observations:' || r.unit_code || ':' || p.series_code || '@' || p.frequency as measure_key,
    coalesce(r.series_name, p.series_name) as measure_name,
    p.series_code as measure_codes,
    r.unit_code,
    r.unit_name,
    p.country_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    p.frequency,
    p.period_start as date_key,
    p.time_code as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join series as r on p.series_code = r.series_code
left join geo_map as g on p.country_code = g.source_code
-- a version that cannot be read loads nothing rather than a period that may not have ended
where p.version_day is not null and p.period_end_next <= p.version_day + interval 1 day
