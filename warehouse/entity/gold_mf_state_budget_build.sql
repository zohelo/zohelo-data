-- Gold build stage of the Ministry of Finance state budget execution reports, by rule (docs/gold-coverage.md): every
-- value of silver.budget_execution, every line of table 1 ("Zestawienie ogólne z wykonania budżetu państwa") of the
-- newest report of each year, in the common observation shape (the 21 columns of poc_build_nbp.sql, same order).
-- Applied by warehouse/observations.py to the table's files as one block. Relations:
--   silver      the data files of silver.budget_execution (year × month × line × measure)
--   measure_key = mf_state_budget:budget_execution:THS_PLN:<measure>:<line label>[#<occurrence>]@<M|A>: the measure as
--                 Silver names it (execution_ytd: execution from January to the month, as the report states it;
--                 plan: the budget act for the year) and the report's line label as published (numbering included);
--                 a label repeated in a block gets its occurrence when above 1 (Silver's grain), so a main line
--                 printed twice is two series, never one picked row. A line whose label changes between reports is
--                 a new series. One frequency per measure: execution_ytd M, plan A.
--   value       = the year-to-date figure as published, placed at its month (the curated stage's rule); it is never
--                 differenced into a monthly flow, and December is not repeated as an annual point
--   unit        = thousand zloty as published (THS_PLN, D33; nothing rescaled)
--   breakdown   = none ('{}', is_total)
--   date_key    = the first day of the month (execution_ytd) or year (plan); period_label 2025-09 or 2025
--   geography   = Poland (PL): the state budget
--   status      = `ok`: the reports carry no flags. The budget act is the law's amount for the year, published as
--                 such, not an estimate of an observation.
-- Not loaded: a value that is not a finite number (Silver keeps numbers only; nothing is 0-filled), an execution
-- row without a month and a plan row with one (neither exists: Silver's tests).
-- The curated stage poc_build_public_finance (the seven main lines, its own keys) stays the KPI layer.
with obs as (
    select
        s.*,
        case s.measure when 'execution_ytd' then 'M' when 'plan' then 'A' end as frequency,
        s.item_label || case when s.item_occurrence > 1 then '#' || s.item_occurrence else '' end as line_code
    from silver as s
    where
        s.value_thousand_pln is not null
        and not isnan(s.value_thousand_pln)
        and not isinf(s.value_thousand_pln)
        and (
            (s.measure = 'execution_ytd' and s.month_end between 1 and 12)
            or (s.measure = 'plan' and s.month_end is null)
        )
)

select
    'mf_state_budget' as source_system,
    'budget_execution' as block_id,
    'Ministry of Finance state budget execution, table 1, every line' as block_name,
    'mf_state_budget:budget_execution:THS_PLN:' || o.measure || ':' || o.line_code || '@' || o.frequency as measure_key,
    o.line_code
    || case o.measure
        when 'execution_ytd' then ', execution from January to the month, thousand zloty'
        else ', budget act, thousand zloty'
    end as measure_name,
    o.measure || ':' || coalesce(o.item_key, o.item_code, '') as measure_codes,
    'THS_PLN' as unit_code,
    'thousand zloty' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    o.frequency,
    case when o.frequency = 'M' then make_date(o.budget_year, o.month_end, 1) else make_date(o.budget_year, 1, 1) end
        as date_key,
    case
        when o.frequency = 'M' then strftime(make_date(o.budget_year, o.month_end, 1), '%Y-%m')
        else cast(o.budget_year as varchar)
    end as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(o.value_thousand_pln as double) as value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
