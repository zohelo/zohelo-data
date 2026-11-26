-- Gold build stage of the ILOSTAT indicator files, by rule (docs/gold-coverage.md): every row of
-- silver.ilostat_observations with a value, every indicator, area, sex, classification and survey, in the
-- common observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by
-- warehouse/observations.py to the table's files as one block. Relations:
--   silver    the data files of silver.ilostat_observations (indicator file × series × period)
--   geo_map   gold.map_geography rows of ILOSTAT (ISO 3166 alpha-3 → country)
--   measure_key = ilostat:ilostat_observations:<unit>:<indicator>@<FREQ>: an indicator of one frequency is
--                 one measure (the file is the indicator and the frequency, so the annual and the quarterly
--                 unemployment rate are two measures).
--   breakdown   = every other dimension of the ILO series key, codes as published: sex, classif1, classif2
--                 (age bands and other classifications) and source (the survey). The survey is part of the
--                 series because the ILO publishes it in the series key: where an area has two surveys for one
--                 period they are two series, never one chosen value. sex_code T, M, F, _O (SEX_O);
--                 is_total when sex is SEX_T and each classification is absent or ends in _TOTAL.
--   unit        = per cent (`PC`, ILOSTAT's unit of the rate indicators ending in _RT); an indicator of
--                 another kind keeps its ILOSTAT suffix as the unit code (reported until it is mapped, D33)
--   date_key    = first day of the period (2025 A, 2025Q2 Q, 2025M07 M); other periods are not loaded
--   geography   = by ref_area through map_geography; the ILO aggregates (X01 …) have no Gold area and are
--                 kept as _X with their code
--   status      = obs_status: none ok, B break, U low reliability, any other code `flagged`
-- Not loaded: a cell without a value (nothing is 0-filled), and a period that had not ended on the day of
-- the file's version (source_version, the download time): such a value is a projection or an incomplete
-- period, not an observation (the ILO aggregates carry years up to two years ahead).
with placed as (
    select
        s.*,
        case
            when s.freq = 'A' and regexp_matches(s.time_period, '^\d{4}$')
                then make_date(cast(s.time_period as integer), 1, 1)
            when s.freq = 'Q' and regexp_matches(s.time_period, '^\d{4}Q[1-4]$')
                then make_date(cast(left(s.time_period, 4) as integer), cast(right(s.time_period, 1) as integer) * 3 - 2, 1)
            when s.freq = 'M' and regexp_matches(s.time_period, '^\d{4}M(0[1-9]|1[0-2])$')
                then make_date(cast(left(s.time_period, 4) as integer), cast(right(s.time_period, 2) as integer), 1)
        end as date_key,
        case s.freq when 'A' then interval 1 year when 'Q' then interval 3 month else interval 1 month end
            as period_length,
        try_cast(
            regexp_replace(regexp_extract(s.source_version, '\d{4}-?\d{2}-?\d{2}'), '^(\d{4})-?(\d{2})-?(\d{2})$', '\1-\2-\3')
            as date
        ) as version_date,
        case when regexp_matches(s.indicator, '_RT$') then 'PC' else regexp_extract(s.indicator, '_([A-Z]+)$', 1) end
            as unit,
        case s.sex when 'SEX_T' then 'T' when 'SEX_M' then 'M' when 'SEX_F' then 'F' when 'SEX_O' then '_O' end
            as sex_code
    from silver as s
    where s.obs_value is not null
)

select
    'ilostat' as source_system,
    'ilostat_observations' as block_id,
    'ILOSTAT indicators (national reported series)' as block_name,
    'ilostat:ilostat_observations:' || p.unit || ':' || p.indicator || '@' || p.freq as measure_key,
    'ILOSTAT ' || p.indicator || ' [' || p.freq || ']' as measure_name,
    p.indicator as measure_codes,
    p.unit as unit_code,
    case p.unit when 'PC' then 'percent' else p.unit end as unit_name,
    p.ref_area as geography_source_code,
    case when p.ref_area is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    p.freq as frequency,
    p.date_key,
    p.time_period as period_label,
    '{'
    || concat_ws(
        ', ',
        '"sex": ' || to_json(p.sex),
        '"classif1": ' || to_json(p.classif1),
        '"classif2": ' || to_json(p.classif2),
        '"source": ' || to_json(p.source_code)
    )
    || '}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    p.sex_code,
    coalesce(p.sex, 'SEX_T') = 'SEX_T'
    and coalesce(p.classif1, '_TOTAL') like '%\_TOTAL' escape '\'
    and coalesce(p.classif2, '_TOTAL') like '%\_TOTAL' escape '\' as is_total,
    p.obs_value as value,
    case coalesce(p.obs_status, '')
        when '' then 'ok' when 'B' then 'break' when 'U' then 'low_reliability' else 'flagged'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.ref_area = g.source_code
where
    p.date_key is not null
    and (p.version_date is null or p.date_key + p.period_length <= p.version_date + interval 1 day)
