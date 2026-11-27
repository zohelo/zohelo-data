-- Gold build stage of the Ministry of Finance local-government budget summaries, by rule (docs/gold-coverage.md):
-- every value of silver.mf_jst_budget_summary, every item of the DW (revenue, expenditure, financing) and ZN
-- (liabilities, receivables) summary workbooks of each unit type and quarter, in the common observation shape (the 21
-- columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one block.
-- Relations:
--   silver      the data files of silver.mf_jst_budget_summary (unit type × quarter × section × item × measure)
--   measure_key = mf_jst_budgets:mf_jst_budget_summary:pln:<measure>:<section>:<item label>[#<occurrence>]@Q: Silver's
--                 measure (plan: the plan after changes; executed: executed from January to the quarter's end; total:
--                 the ZN stock at the quarter's end), its section and the item as published (whitespace collapsed by
--                 Silver; the ZN items are their report codes E, E1, N1, ...); a label repeated in a section gets its
--                 occurrence when above 1 (Silver's grain), so "w tym: inwestycyjne" under two parents is two series,
--                 never one picked row. A label that changes between workbooks is a new series. One frequency: Q.
--   value       = the amount as published, PLN (D33; nothing rescaled), placed at its quarter; a year-to-date figure is
--                 never differenced into a quarterly flow (the curated stage's rule)
--   breakdown   = the unit type ({"unit_type": jst|gminy|miasta|powiaty|wojew|zwiazki}); jst (all units, the
--                 workbooks' own total) is the total series (is_total)
--   date_key    = the first day of the quarter; period_label 2025-Q3
--   geography   = Poland (PL): national totals per unit type
--   status      = `ok`: the workbooks carry no flags. The plan after changes is the amount the units' budget
--                 resolutions set, published as such, not an estimate of an observation (as the state budget's plan).
-- Not loaded: a value that is not a finite number (Silver keeps numbers only; a cell "#" is no row, nothing is
-- 0-filled), a quarter after the workbook's version (a projection; none today).
-- The curated stage poc_build_mf_jst_budgets (executed revenue, expenditure, result and liabilities, its own keys)
-- stays the KPI layer.
with obs as (
    select
        s.*,
        s.item_label || case when s.item_occurrence > 1 then '#' || s.item_occurrence else '' end as item_code
    from silver as s
    where
        s.value_pln is not null
        and not isnan(s.value_pln)
        and not isinf(s.value_pln)
        and s.period_date is not null
        -- source_version is the download time, 20261112T150548Z (gov.pl sends no Last-Modified)
        and s.period_date <= coalesce(cast(try_strptime(left(s.source_version, 8), '%Y%m%d') as date), s.period_date)
)

select
    'mf_jst_budgets' as source_system,
    'mf_jst_budget_summary' as block_id,
    'MF local-government budget summaries, every item (year to date)' as block_name,
    'mf_jst_budgets:mf_jst_budget_summary:pln:' || o.measure_code || ':' || o.section || ':' || o.item_code || '@Q'
        as measure_key,
    o.section
    || ': '
    || o.item_code
    || case o.measure_code
        when 'plan' then ', plan after changes, PLN'
        when 'executed' then ', executed from January to the quarter''s end, PLN'
        else ', total at the quarter''s end, PLN'
    end as measure_name,
    o.measure_code || ':' || o.section || ':' || o.report_family as measure_codes,
    'pln' as unit_code,
    'PLN' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'Q' as frequency,
    o.period_date as date_key,
    o.report_year || '-Q' || o.report_quarter as period_label,
    cast(to_json(map(['unit_type'], [o.unit_type])) as varchar) as breakdown_json,
    cast(to_json(map(['unit_type'], [o.unit_type])) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.unit_type = 'jst' as is_total,
    cast(o.value_pln as double) as value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
