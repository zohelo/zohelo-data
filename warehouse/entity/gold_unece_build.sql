-- Gold build stage of the UNECE Statistical Database tables, by rule (docs/gold-coverage.md): every row of
-- silver.unece_observations with a value, every table, series and area, in the common observation shape
-- (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's
-- files as one block. Relations:
--   silver    the data files of silver.unece_observations (table × series × period)
--   geo_map   gold.map_geography rows of UNECE (UN M49 country codes, E27, EAA)
--   measure_key = unece:unece_observations:_Z:<table id>[:<codes>]@<freq>: the value codes of every variable of
--                 the table other than the area, joined by '.' in the table's variable order (indicator,
--                 measurement, price base, adjustment: PXWeb tables carry only measure variables and the
--                 area), so no series has a breakdown. The table id is the table as landed without its part
--                 suffix. measure_name = the table title and the value labels.
--   unit        = `as named`: PXWeb has no unit attribute; the unit is in the value labels (`Thousands of
--                 persons`, `2021=100`, `percent`), reported as unmapped until a unit_map row names it (D33)
--   date_key    = first day of the period (1990 A, 2025Q2 Q, 2025M07 M); other periods are not loaded
--   geography   = by area code through map_geography; the UNECE groupings (CIS, CWG, EAEU, ECE, NA, ST7) have
--                 no Gold area and are kept as _X with their code
--   status      = `ok`, or `flagged` when the cell carries a JSON-stat status
-- Not loaded: a cell without a value; a period that had not ended on the day of the table's version (the
-- table's "updated" time): a projection or an incomplete period. A 0 in a series whose value labels (the
-- table title for a table whose only variable is the area) name no rate,
-- growth, change, per cent or balance (an index, a level in persons or money, an exchange rate) cannot be a
-- measured value: it is a value below the printed precision or a cell without a value and is not loaded,
-- as in the curated build stage (zero_is_missing, 148 cells 1990-1997, Dev trial 43306220004); a rate of 0 is
-- kept.
with placed as (
    select
        s.*,
        case
            when regexp_matches(s.time_period, '^\d{4}$') then 'A'
            when regexp_matches(s.time_period, '^\d{4}Q[1-4]$') then 'Q'
            when regexp_matches(s.time_period, '^\d{4}M(0[1-9]|1[0-2])$') then 'M'
        end as freq,
        case
            when regexp_matches(s.time_period, '^\d{4}$') then make_date(cast(s.time_period as integer), 1, 1)
            when regexp_matches(s.time_period, '^\d{4}Q[1-4]$')
                then make_date(cast(left(s.time_period, 4) as integer), cast(right(s.time_period, 1) as integer) * 3 - 2, 1)
            when regexp_matches(s.time_period, '^\d{4}M(0[1-9]|1[0-2])$')
                then make_date(cast(left(s.time_period, 4) as integer), cast(right(s.time_period, 2) as integer), 1)
        end as date_key,
        try_cast(
            regexp_replace(regexp_extract(s.source_version, '\d{4}-?\d{2}-?\d{2}'), '^(\d{4})-?(\d{2})-?(\d{2})$', '\1-\2-\3')
            as date
        ) as version_date,
        list_filter(json_keys(s.dimensions_json), lambda k: k not in ('Country', 'Country/Region')) as variables
    from silver as s
    where s.obs_value is not null
),

coded as (
    select
        p.*,
        array_to_string(list_transform(p.variables, lambda k: json_extract_string(p.dimensions_json, '$."' || k || '"')), '.')
            as codes,
        array_to_string(list_transform(p.variables, lambda k: json_extract_string(p.labels_json, '$."' || k || '"')), ', ')
            as labels,
        case p.freq when 'A' then interval 1 year when 'Q' then interval 3 month else interval 1 month end as period_length
    from placed as p
    where p.date_key is not null
)

select
    'unece' as source_system,
    'unece_observations' as block_id,
    'UNECE Statistical Database, economic statistics' as block_name,
    'unece:unece_observations:_Z:' || c.table_id || case when c.codes = '' then '' else ':' || c.codes end || '@' || c.freq
        as measure_key,
    c.table_title || case when coalesce(c.labels, '') = '' then '' else ' — ' || c.labels end || ' [' || c.freq || ']'
        as measure_name,
    c.table_id || case when c.codes = '' then '' else ':' || c.codes end as measure_codes,
    'as named' as unit_code,
    'as named by the value labels' as unit_name,
    c.area_code as geography_source_code,
    case when c.area_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    c.freq as frequency,
    c.date_key,
    c.time_period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    c.obs_value as value,
    case when c.obs_status is null then 'ok' else 'flagged' end as observation_status_key,
    c.source_version,
    c.raw_sha256
from coded as c
left join geo_map as g on c.area_code = g.source_code
where
    (c.version_date is null or c.date_key + c.period_length <= c.version_date + interval 1 day)
    and not (
        c.obs_value = 0
        and not regexp_matches(
            replace(lower(coalesce(nullif(c.labels, ''), c.table_title, '')), 'exchange rate', 'exchange'),
            'rate|growth|change|per ?cent|%|balance'
        )
    )
