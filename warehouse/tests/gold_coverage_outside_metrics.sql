-- Block-scoped metrics (#3204 PR A2): warehouse/changed.py OUTSIDE_METRICS does not rebuild a metrics model when only
-- gold.coverage changed. That is safe only while no coverage row can reach a metrics row: coverage enters
-- poc_dim_measure as activated = false rows, poc_concept_measures keeps activated measures, and every metrics model
-- joins poc_dim_measure on the measure keys of the facts and of its concept measures. Fails on
-- (a) a measure_key that has a coverage row and an activated row in poc_dim_measure (the joins would pick up the
--     coverage row: a block whose coverage block_id differs from the build's block_id), or
-- (b) a concept measure without an activated row in poc_dim_measure.
-- A kpi_measure pattern that matches a coverage key is not checked: it reaches no metrics row through (b), and a KPI
-- whose measure is not activated yet matches its coverage key on purpose. Fix the data or the model; if coverage must
-- reach the metrics, remove gold_coverage.coverage from OUTSIDE_METRICS, never this test.
select
    'coverage row shares an activated measure_key' as problem,
    measure_key
from {{ ref('poc_dim_measure') }}
group by measure_key
having bool_or(activated) and bool_or(not activated)
union all
select
    'concept measure is not an activated measure' as problem,
    cm.measure_key
from {{ ref('poc_concept_measures') }} as cm
where not exists (
    select 1
    from {{ ref('poc_dim_measure') }} as m
    where m.measure_key = cm.measure_key and m.activated
)
