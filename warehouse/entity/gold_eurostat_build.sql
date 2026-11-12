-- Gold build stage of ONE Eurostat dataset, by rule (docs/gold-coverage.md): every row of the dataset's
-- Silver file with a value, in the common observation shape (the 21 columns of poc_build_nbp.sql, same
-- order). Applied by warehouse/observations.py to one Silver file at a time, after the two steps that
-- make the tables `dimension_sets` (gold_eurostat_dimension_sets.sql: the measure and total rules) and
-- `measure_names` (gold_eurostat_measure_names.sql). Relations:
--   silver    the dataset's file of silver.eurostat_observations
--   geo_map   gold.map_geography rows of Eurostat (source_code, geography_key)
--   flows, codes   silver.eurostat_dataflows / eurostat_codes (names)
-- Every row is shaped on its own with these small lookups: nothing is grouped by series, so memory
-- does not grow with the number of series.
--   measure_key = eurostat:<dataset>:<unit>[:<measure codes>]@<frequency>; frequency is part of the key
--                 because series_key has none and two frequencies of one measure would collide on date_key
--   date_key    = first day of the period (A, S, Q, M, W = ISO Monday, D); other periods are not loaded
--   geography   = by code through map_geography; no geo dimension → _Z; unmatched → _X, code kept
--   status      = Eurostat flag (first letter); every row of a projection dataset (PROJ_*) is `forecast`
with placed as (
    select
        s.*,
        upper(s.dataset_id) as block_id,
        case
            when s.freq_code = 'A' and regexp_matches(s.time_period, '^\d{4}$')
                then make_date(cast(s.time_period as integer), 1, 1)
            when s.freq_code = 'S' and regexp_matches(s.time_period, '^\d{4}-S[12]$')
                then
                    make_date(
                        cast(substr(s.time_period, 1, 4) as integer),
                        (cast(substr(s.time_period, 7, 1) as integer) - 1) * 6 + 1,
                        1
                    )
            when s.freq_code = 'Q' and regexp_matches(s.time_period, '^\d{4}-Q[1-4]$')
                then
                    make_date(
                        cast(substr(s.time_period, 1, 4) as integer),
                        (cast(substr(s.time_period, 7, 1) as integer) - 1) * 3 + 1,
                        1
                    )
            when s.freq_code = 'M' and regexp_matches(s.time_period, '^\d{4}-(0[1-9]|1[0-2])$')
                then cast(s.time_period || '-01' as date)
            when s.freq_code = 'W' and regexp_matches(s.time_period, '^\d{4}-W\d{2}$')
                then cast(try_strptime(s.time_period || '-1', '%G-W%V-%u') as date)
            when s.freq_code = 'D' then try_cast(s.time_period as date)
        end as date_key
    from silver as s
    where s.value is not null
),

split as (
    select
        p.*,
        coalesce(
            array_to_string(
                list_transform(
                    d.measure_positions, lambda i: string_split(p.breakdown_codes, ',')[i]  -- noqa: RF02
                ),
                ':'
            ),
            ''
        ) as measure_codes,
        coalesce(
            cast(
                to_json(
                    map(
                        d.breakdown_dimension_ids,
                        list_transform(
                            d.breakdown_positions, lambda i: string_split(p.breakdown_codes, ',')[i]  -- noqa: RF02
                        )
                    )
                ) as varchar
            ),
            '{}'
        ) as breakdown_json,
        coalesce(
            list_transform(
                d.breakdown_positions, lambda i: string_split(p.breakdown_codes, ',')[i]  -- noqa: RF02
            )
            = d.breakdown_totals,
            true
        ) as is_total,
        string_split(p.breakdown_codes, ',')[d.sex_position] as sex_code_raw
    from placed as p
    left join dimension_sets as d on p.breakdown_dimensions = d.breakdown_dimensions
),

code_names as (
    select
        codelist_id,
        code,
        any_value(name_en) as name_en
    from codes
    group by codelist_id, code
),

block_names as (
    select
        upper(dataset_id) as block_id,
        any_value(name_en) as name_en
    from flows
    group by upper(dataset_id)
)

select
    'eurostat' as source_system,
    p.block_id,
    coalesce(f.name_en, p.block_id) as block_name,
    'eurostat:'
    || p.block_id
    || ':'
    || coalesce(p.unit_code, '_Z')
    || case when p.measure_codes = '' then '' else ':' || p.measure_codes end
    || '@'
    || p.freq_code as measure_key,
    coalesce(f.name_en, p.block_id)
    || ' — '
    || coalesce(u.name_en, p.unit_code, 'as named by the measure')
    || coalesce(' — ' || n.codes_name, '')
    || ' ['
    || p.freq_code
    || ']' as measure_name,
    p.measure_codes,
    coalesce(p.unit_code, '_Z') as unit_code,
    coalesce(u.name_en, p.unit_code, 'as named by the measure') as unit_name,
    p.geo_code as geography_source_code,
    case when p.geo_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    p.freq_code as frequency,
    p.date_key,
    p.time_period as period_label,
    p.breakdown_json,
    -- labels of the breakdown codes are not repeated on every row: the code lists name them
    cast(null as varchar) as breakdown_label_json,
    case when p.sex_code_raw in ('F', 'M', 'T') then p.sex_code_raw end as sex_code,
    p.is_total,
    p.value,
    -- projection datasets (PROJ_*) are projections in every row, whatever their flag: never observations
    case
        when p.block_id like 'PROJ\_%' escape '\' then 'forecast'
        else
            case substr(nullif(trim(p.status_code), ''), 1, 1)
                when 'p' then 'provisional' when 'e' then 'estimated' when 'c' then 'confidential' when 'b' then 'break'
                when 'u' then 'low_reliability' when 'd' then 'definition_differs' when 'f' then 'forecast'
                when 's' then 'estimated' when 'n' then 'not_significant' else 'ok'
            end
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from split as p
left join measure_names as n
    on
        p.freq_code = n.freq_code
        and p.unit_code is not distinct from n.unit_code
        and p.measure_codes = n.measure_codes
left join code_names as u on u.codelist_id = 'UNIT' and p.unit_code = u.code
left join block_names as f on p.block_id = f.block_id
left join geo_map as g on p.geo_code = g.source_code
where p.date_key is not null
