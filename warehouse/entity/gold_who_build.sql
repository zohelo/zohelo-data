-- Gold build stage of the WHO data.who.int indicator files, by rule (docs/gold-coverage.md): every row of
-- silver.who_observations with a value, both indicators and every area WHO publishes them for, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the
-- table's files as one block. Relations:
--   silver    the data files of silver.who_observations (indicator × area × year × sex, newest copy of each file)
--   geo_map   the result of gold_who_map.sql: COUNTRY (and territory) M49 codes -> countries, GLOBAL:1 -> World
--   measure_key = who:who_observations:years:<indicator code>@A (WHOSIS_000001 life expectancy at birth,
--                 WHOSIS_000002 healthy life expectancy at birth); the sexes are the breakdown {"sex": "T|F|M"}
--                 (T = both sexes, is_total), never measures of their own
--   unit        = years, as published (D33)
--   date_key    = 1 January of the year (the files hold years only, DIM_TIME_TYPE YEAR)
--   geography   = `<area type>:<M49 code>` through the map: countries by ISO 3166-1; WHO regions, World Bank income
--                 groups and the other WHO groupings stay _X with their code
--   value       = the point estimate (AMOUNT_N); the lower and upper bounds stay in Silver (as the curated stage)
--   status      = ok (the files carry no flags; every row is published, publish_state PUBLISHED)
-- Not loaded: a row without a value (nothing is 0-filled), a row not in state PUBLISHED, a period that is not a
-- year or a year outside 1800–2200, and a year after the year of the file's version (a projection; WHO publishes
-- none in these files).
with placed as (
    select
        s.*,
        case upper(s.sex) when 'TOTAL' then 'T' when 'BOTHSEXES' then 'T' when 'FEMALE' then 'F' when 'MALE' then 'M'
        end as sex_code,
        s.geo_type || ':' || s.geo_code_m49 as area_code,
        try_cast(nullif(regexp_extract(s.source_version, '(19|20)\d{2}'), '') as integer) as version_year
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        and s.publish_state = 'PUBLISHED'
        and s.time_type = 'YEAR'
        and s.year between 1800 and 2200
)

select
    'who' as source_system,
    'who_observations' as block_id,
    'WHO data.who.int: life expectancy and healthy life expectancy at birth, every area' as block_name,
    'who:who_observations:years:' || p.indicator_code || '@A' as measure_key,
    p.indicator_name as measure_name,
    p.indicator_code as measure_codes,
    'years' as unit_code,
    'years' as unit_name,
    p.area_code as geography_source_code,
    case when p.geo_code_m49 is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    'A' as frequency,
    make_date(p.year, 1, 1) as date_key,
    cast(p.year as varchar) as period_label,
    '{"sex": "' || coalesce(p.sex_code, p.sex) || '"}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    p.sex_code,
    coalesce(p.sex_code = 'T', false) as is_total,
    p.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.area_code = g.source_code
where p.version_year is null or p.year <= p.version_year
