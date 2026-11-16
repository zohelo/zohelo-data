-- GUNB RWDZ build stage: every count of building-permit decisions is a whole number between 1 and 100,000 a month.
-- Poland is the largest area: at most 34,105 a month in 2020-01 to 2026-10 (Dev trial 43804193304). A value above
-- the bound means records counted several times (one per plot line) or a wrong area rule; a zero or a fraction means
-- a cell that is not a count. A live test on the build stage, as no metrics model of this KPI is in
-- tests/metrics_value_range.sql yet. Returns the rows outside.
select
    measure_key,
    geography_key,
    date_key,
    breakdown_json,
    value
from {{ ref('poc_build_gunb_rwdz') }}
where value < 1 or value > 1e5 or value <> floor(value)
