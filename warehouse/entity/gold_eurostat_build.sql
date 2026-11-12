-- Gold build stage of ONE Eurostat dataset, by rule (docs/gold-coverage.md): every row of the dataset's
-- Silver file with a value, in the common observation shape (the 21 columns of poc_build_nbp.sql, same
-- order). Applied by warehouse/observations.py to one Silver file at a time; relations:
--   silver    the dataset's file of silver.eurostat_observations
--   geo_map   gold.map_geography rows of Eurostat (source_code, geography_key)
--   flows, dims, codes   silver.eurostat_dataflows / eurostat_dimensions / eurostat_codes (names)
-- Rules, the same for every dataset (no per-dataset list):
--   measure    = dataset × unit × the codes of the measure-defining dimensions (the indicator-like
--                dimensions named in `measure_rule`) × frequency; frequency is part of the key because
--                series_key has none and two frequencies of one measure would collide on date_key
--   breakdown  = every other dimension, source codes as published
--   total      = each breakdown dimension holds its total code: TOTAL, T or _T when the dataset has
--                one for that dimension, the only code when the dimension has one code; a dimension
--                with several codes and no total code has no total series
--   date_key   = first day of the period (A, S, Q, M, W = ISO Monday, D); other periods are not loaded
--   geography  = by code through map_geography; no geo dimension → _Z; unmatched → _X, code kept
with measure_rule as (
    select
        '^(indic|indic_[a-z0-9_]+|na_item|bop_item|nrg_bal|stk_flow|fdi_item|int_rt|exch_rt|airpol|statinfo'
        || '|tra_meas|ind_type|finpos|co_nco|wst_oper|indicator|item|variable|lcstruct|asset10|asset|s_adj)$' as pattern
),

-- not materialized: the rows are streamed twice (shapes, final join) instead of held in memory
placed as not materialized (
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

-- Everything below works per row with small lookups only (a few dimensions, a few thousand measures):
-- no row set is grouped by series, so memory does not grow with the number of series
-- (AVIA_TF_ACA: 492 M rows with a value and no geo dimension).
dimension_codes as (  -- the codes each breakdown dimension holds in this dataset
    select distinct
        unnest(string_split(breakdown_dimensions, ',')) as dimension_id,
        unnest(string_split(breakdown_codes, ',')) as code
    from placed
    where coalesce(breakdown_dimensions, '') <> ''
),

dimension_totals as (  -- the total code of each breakdown dimension in this dataset, if it has one
    select
        dimension_id,
        regexp_matches(dimension_id, (select r.pattern from measure_rule as r)) as is_measure,
        case
            when bool_or(code = 'TOTAL') then 'TOTAL'
            when bool_or(code = 'T') then 'T'
            when bool_or(code = '_T') then '_T'
            when count(*) = 1 then any_value(code)
        end as total_code
    from dimension_codes
    group by dimension_id
),

dimension_sets as (  -- one row per dimension list (one per dataset in practice): positions of each kind
    select
        l.breakdown_dimensions,
        coalesce(list(l.pos order by l.pos) filter (where t.is_measure), []) as measure_positions,
        coalesce(list(l.pos order by l.pos) filter (where not t.is_measure), []) as breakdown_positions,
        coalesce(list(l.dimension_id order by l.pos) filter (where not t.is_measure), []) as breakdown_dimension_ids,
        coalesce(list(t.total_code order by l.pos) filter (where not t.is_measure), []) as breakdown_totals,
        max(case when l.dimension_id = 'sex' then l.pos end) as sex_position
    from (
        select
            d.breakdown_dimensions,
            unnest(string_split(d.breakdown_dimensions, ',')) as dimension_id,
            generate_subscripts(string_split(d.breakdown_dimensions, ','), 1) as pos
        from (select distinct p.breakdown_dimensions from placed as p where coalesce(p.breakdown_dimensions, '') <> '') as d
    ) as l
    inner join dimension_totals as t on l.dimension_id = t.dimension_id
    group by l.breakdown_dimensions
),

split as (
    select
        p.*,
        coalesce(
            array_to_string(
                list_transform(d.measure_positions, lambda i: string_split(p.breakdown_codes, ',')[i]), ':'
            ),
            ''
        ) as measure_codes,
        coalesce(
            cast(
                to_json(
                    map(
                        d.breakdown_dimension_ids,
                        list_transform(d.breakdown_positions, lambda i: string_split(p.breakdown_codes, ',')[i])
                    )
                ) as varchar
            ),
            '{}'
        ) as breakdown_json,
        coalesce(
            list_transform(d.breakdown_positions, lambda i: string_split(p.breakdown_codes, ',')[i])
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

dimension_lists as (
    select
        dimension_id,
        any_value(codelist_id) as codelist_id
    from dims
    where upper(dataset_id) in (select distinct p.block_id from placed as p)
    group by dimension_id
),

measure_shapes as (  -- one row per measure: few, whatever the number of series
    select distinct
        freq_code,
        unit_code,
        breakdown_dimensions,
        measure_codes
    from split
),

measure_names as (  -- names of the measure codes, from the dataset's code lists
    select
        m.freq_code,
        m.unit_code,
        m.breakdown_dimensions,
        m.measure_codes,
        string_agg(coalesce(n.name_en, x.code), ' — ' order by x.pos) as codes_name
    from measure_shapes as m
    inner join dimension_sets as d on m.breakdown_dimensions = d.breakdown_dimensions
    cross join lateral (
        select
            unnest(string_split(m.measure_codes, ':')) as code,
            unnest(d.measure_positions) as dimension_position,
            generate_subscripts(d.measure_positions, 1) as pos
    ) as x
    left join dimension_lists as l
        on string_split(m.breakdown_dimensions, ',')[x.dimension_position] = l.dimension_id
    left join code_names as n on l.codelist_id = n.codelist_id and x.code = n.code
    where m.measure_codes <> ''
    group by m.freq_code, m.unit_code, m.breakdown_dimensions, m.measure_codes
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
    case substr(nullif(trim(p.status_code), ''), 1, 1)
        when 'p' then 'provisional' when 'e' then 'estimated' when 'c' then 'confidential' when 'b' then 'break'
        when 'u' then 'low_reliability' when 'd' then 'definition_differs' when 'f' then 'forecast'
        when 's' then 'estimated' when 'n' then 'not_significant' else 'ok'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from split as p
left join measure_names as n
    on
        p.freq_code = n.freq_code
        and p.unit_code is not distinct from n.unit_code
        and p.breakdown_dimensions = n.breakdown_dimensions
        and p.measure_codes = n.measure_codes
left join code_names as u on u.codelist_id = 'UNIT' and p.unit_code = u.code
left join block_names as f on p.block_id = f.block_id
left join geo_map as g on p.geo_code = g.source_code
where p.date_key is not null
