-- Gold build stage of the OECD Data Explorer selection, by rule (docs/gold-coverage.md): every row of
-- silver.oecd_cli (dataflow DSD_STES@DF_CLI) with a value, every measure and every area, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by
-- warehouse/observations.py to the table's files as one block. Relations:
--   silver    the data files of silver.oecd_cli (series × period)
--   geo_map   gold.map_geography rows of OECD (source_code, geography_key)
--   measure_key = oecd:oecd_cli:<UNIT_MEASURE>:<MEASURE>:<ADJUSTMENT>:<TRANSFORMATION>:<METHODOLOGY>@<FREQ>:
--                 every dimension of the dataflow other than the area and the frequency defines the measure
--                 (an amplitude-adjusted index and its growth rate are not one measure broken down), so no
--                 series has a breakdown. The frequency ends the key because series_key has none.
--   date_key    = first day of the period (A, Q, M); other periods are not loaded
--   geography   = by REF_AREA through map_geography; unmatched → _X, code kept
--   status      = SDMX OBS_STATUS: A or none ok, P provisional, E estimated, F forecast, B break,
--                 D definition differs, U low reliability, N not significant; any other code `flagged`
-- A period without a number has no value in Silver and is not loaded. No dictionary of OECD is loaded, so
-- names are the published codes.
with placed as (
    select
        s.*,
        case
            when s.freq = 'A' and regexp_matches(s.time_period, '^\d{4}$')
                then make_date(cast(s.time_period as integer), 1, 1)
            when s.freq = 'Q' and regexp_matches(s.time_period, '^\d{4}-Q[1-4]$')
                then
                    make_date(
                        cast(substr(s.time_period, 1, 4) as integer),
                        (cast(substr(s.time_period, 7, 1) as integer) - 1) * 3 + 1,
                        1
                    )
            when s.freq = 'M' and regexp_matches(s.time_period, '^\d{4}-(0[1-9]|1[0-2])$')
                then cast(s.time_period || '-01' as date)
        end as date_key,
        coalesce(s.measure, '_Z')
        || ':'
        || coalesce(s.adjustment, '_Z')
        || ':'
        || coalesce(s.transformation, '_Z')
        || ':'
        || coalesce(s.methodology, '_Z') as measure_codes
    from silver as s
    where s.obs_value is not null
)

select
    'oecd' as source_system,
    'oecd_cli' as block_id,
    'OECD composite leading indicators and confidence indicators (DSD_STES@DF_CLI)' as block_name,
    'oecd:oecd_cli:' || coalesce(p.unit_measure, '_Z') || ':' || p.measure_codes || '@' || p.freq as measure_key,
    'OECD composite leading indicators and confidence indicators — '
    || p.measure_codes
    || ' — '
    || coalesce(p.unit_measure, 'as named by the measure')
    || ' ['
    || p.freq
    || ']' as measure_name,
    p.measure_codes,
    coalesce(p.unit_measure, '_Z') as unit_code,
    coalesce(p.unit_measure, 'as named by the measure') as unit_name,
    p.ref_area as geography_source_code,
    case when p.ref_area is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    p.freq as frequency,
    p.date_key,
    p.time_period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.obs_value as value,
    case coalesce(p.obs_status, 'A')
        when 'A' then 'ok' when 'P' then 'provisional' when 'E' then 'estimated' when 'F' then 'forecast'
        when 'B' then 'break' when 'D' then 'definition_differs' when 'U' then 'low_reliability'
        when 'N' then 'not_significant' else 'flagged'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.ref_area = g.source_code
where p.date_key is not null
