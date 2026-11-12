-- Gold build stage of ONE Eurostat dataset, by rule (docs/gold-coverage.md): every row of the dataset's
-- Silver file with a value, in the common observation shape (the 21 columns of poc_build_nbp.sql, same
-- order). Applied by warehouse/observations.py to one Silver file at a time; relations:
--   silver    the dataset's file of silver.eurostat_observations
--   geo_map   gold.map_geography rows of Eurostat (source_code, geography_key)
--   flows, dims, codes   silver.eurostat_dataflows / eurostat_dimensions / eurostat_codes (labels)
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

placed as (
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

shapes as (  -- one row per unit × breakdown codes × frequency: the per-series work is done here, not per row
    select distinct
        block_id,
        freq_code,
        unit_code,
        coalesce(breakdown_dimensions, '') as breakdown_dimensions,
        coalesce(breakdown_codes, '') as breakdown_codes
    from placed
),

pairs as (
    select
        s.*,
        unnest(string_split(s.breakdown_dimensions, ',')) as dimension_id,
        unnest(string_split(s.breakdown_codes, ',')) as code,
        generate_subscripts(string_split(s.breakdown_dimensions, ','), 1) as pos
    from shapes as s
    where s.breakdown_dimensions <> ''
),

dimension_totals as (  -- the total code of each breakdown dimension in this dataset, if it has one
    select
        dimension_id,
        case
            when bool_or(code = 'TOTAL') then 'TOTAL'
            when bool_or(code = 'T') then 'T'
            when bool_or(code = '_T') then '_T'
            when count(distinct code) = 1 then any_value(code)
        end as total_code
    from pairs
    group by dimension_id
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
    where upper(dataset_id) = (select any_value(p.block_id) from placed as p)
    group by dimension_id
),

named as (
    select
        p.*,
        regexp_matches(p.dimension_id, (select r.pattern from measure_rule as r)) as is_measure,
        coalesce(n.name_en, p.code) as code_name,
        coalesce(p.code = t.total_code, false) as is_total_code
    from pairs as p
    left join dimension_totals as t on p.dimension_id = t.dimension_id
    left join dimension_lists as d on p.dimension_id = d.dimension_id
    left join code_names as n on d.codelist_id = n.codelist_id and p.code = n.code
),

shaped as (
    select
        block_id,
        freq_code,
        unit_code,
        breakdown_dimensions,
        breakdown_codes,
        coalesce(string_agg(code, ':' order by pos) filter (where is_measure), '') as measure_codes,
        string_agg(code_name, ' — ' order by pos) filter (where is_measure) as codes_name,
        cast(
            to_json(
                map(
                    coalesce(list(dimension_id order by pos) filter (where not is_measure), []),
                    coalesce(list(code order by pos) filter (where not is_measure), [])
                )
            ) as varchar
        ) as breakdown_json,
        cast(
            to_json(
                map(
                    coalesce(list(dimension_id order by pos) filter (where not is_measure), []),
                    coalesce(list(code_name order by pos) filter (where not is_measure), [])
                )
            ) as varchar
        ) as breakdown_label_json,
        coalesce(bool_and(is_total_code) filter (where not is_measure), true) as is_total,
        max(case when dimension_id = 'sex' and code in ('F', 'M', 'T') then code end) as sex_code
    from named
    group by block_id, freq_code, unit_code, breakdown_dimensions, breakdown_codes
),

series as (  -- shapes without any breakdown dimension keep their row
    select
        s.block_id,
        s.freq_code,
        s.unit_code,
        s.breakdown_dimensions,
        s.breakdown_codes,
        coalesce(x.measure_codes, '') as measure_codes,
        x.codes_name,
        coalesce(x.breakdown_json, '{}') as breakdown_json,
        coalesce(x.breakdown_label_json, '{}') as breakdown_label_json,
        coalesce(x.is_total, true) as is_total,
        x.sex_code,
        coalesce(u.name_en, s.unit_code, 'as named by the measure') as unit_name,
        coalesce(f.name_en, s.block_id) as block_name
    from shapes as s
    left join shaped as x
        on
            s.freq_code = x.freq_code
            and s.unit_code is not distinct from x.unit_code
            and s.breakdown_dimensions = x.breakdown_dimensions
            and s.breakdown_codes = x.breakdown_codes
    left join code_names as u on u.codelist_id = 'UNIT' and s.unit_code = u.code
    left join (
        select
            upper(dataset_id) as block_id,
            any_value(name_en) as name_en
        from flows
        group by upper(dataset_id)
    ) as f on s.block_id = f.block_id
)

select
    'eurostat' as source_system,
    p.block_id,
    s.block_name,
    'eurostat:'
    || p.block_id
    || ':'
    || coalesce(p.unit_code, '_Z')
    || case when s.measure_codes = '' then '' else ':' || s.measure_codes end
    || '@'
    || p.freq_code as measure_key,
    s.block_name || ' — ' || s.unit_name || coalesce(' — ' || s.codes_name, '') || ' [' || p.freq_code || ']'
        as measure_name,
    s.measure_codes,
    coalesce(p.unit_code, '_Z') as unit_code,
    s.unit_name,
    p.geo_code as geography_source_code,
    case when p.geo_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    p.freq_code as frequency,
    p.date_key,
    p.time_period as period_label,
    s.breakdown_json,
    s.breakdown_label_json,
    s.sex_code,
    s.is_total,
    p.value,
    case substr(nullif(trim(p.status_code), ''), 1, 1)
        when 'p' then 'provisional' when 'e' then 'estimated' when 'c' then 'confidential' when 'b' then 'break'
        when 'u' then 'low_reliability' when 'd' then 'definition_differs' when 'f' then 'forecast'
        when 's' then 'estimated' when 'n' then 'not_significant' else 'ok'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
inner join series as s
    on
        p.freq_code = s.freq_code
        and p.unit_code is not distinct from s.unit_code
        and coalesce(p.breakdown_dimensions, '') = s.breakdown_dimensions
        and coalesce(p.breakdown_codes, '') = s.breakdown_codes
left join geo_map as g on p.geo_code = g.source_code
where p.date_key is not null
