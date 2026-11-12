-- Gold build stage of one Eurostat dataset, step 1 (docs/gold-coverage.md): one row per dimension list
-- of the dataset (one in practice) with the positions of its measure-defining and breakdown dimensions
-- and each breakdown dimension's total code. Kept as the table `dimension_sets` for the next steps, so
-- the planner knows it is tiny (as a CTE it was taken for a table as large as the dataset and
-- AVIA_TF_ACA filled 75 GB of temp space, Dev trials 43244012890 and 43251674524).
--   measure    = dataset × unit × the codes of the measure-defining dimensions (the indicator-like
--                dimensions named in `measure_rule`, and s_adj: seasonal adjustment belongs to the measure)
--   breakdown  = every other dimension, source codes as published
--   total      = each breakdown dimension holds its total code: TOTAL, T or _T when the dataset has
--                one for that dimension, the only code when the dimension has one code; a dimension
--                with several codes and no total code has no total series
with measure_rule as (
    select
        '^(indic|indic_[a-z0-9_]+|na_item|bop_item|nrg_bal|stk_flow|fdi_item|int_rt|exch_rt|airpol|statinfo'
        || '|tra_meas|ind_type|finpos|co_nco|wst_oper|indicator|item|variable|lcstruct|asset10|asset|s_adj)$' as pattern
),

dimension_codes as (  -- the codes each breakdown dimension holds in this dataset
    select distinct
        unnest(string_split(breakdown_dimensions, ',')) as dimension_id,
        unnest(string_split(breakdown_codes, ',')) as code
    from silver
    where value is not null and coalesce(breakdown_dimensions, '') <> ''
),

dimension_totals as (
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

dimension_lists as (
    select distinct breakdown_dimensions
    from silver
    where value is not null and coalesce(breakdown_dimensions, '') <> ''
),

positions as (
    select
        d.breakdown_dimensions,
        unnest(string_split(d.breakdown_dimensions, ',')) as dimension_id,
        generate_subscripts(string_split(d.breakdown_dimensions, ','), 1) as pos
    from dimension_lists as d
)

select
    l.breakdown_dimensions,
    coalesce(list(l.pos order by l.pos) filter (where t.is_measure), []) as measure_positions,
    coalesce(list(l.pos order by l.pos) filter (where not t.is_measure), []) as breakdown_positions,
    coalesce(list(l.dimension_id order by l.pos) filter (where t.is_measure), []) as measure_dimension_ids,
    coalesce(list(l.dimension_id order by l.pos) filter (where not t.is_measure), []) as breakdown_dimension_ids,
    coalesce(list(t.total_code order by l.pos) filter (where not t.is_measure), []) as breakdown_totals,
    max(case when l.dimension_id = 'sex' then l.pos end) as sex_position
from positions as l
inner join dimension_totals as t on l.dimension_id = t.dimension_id
group by l.breakdown_dimensions
