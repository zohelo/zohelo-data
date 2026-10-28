-- Gold PoC: a fact holds one row per series and period (a duplicate means two source units
-- mapped to one area, or one series counted twice). Fails on any duplicate.
select
    series_key,
    date_key,
    count(*) as rows_per_key
from (
    select
        series_key,
        date_key
    from {{ ref('poc_fact_annual') }}
    union all
    select
        series_key,
        date_key
    from {{ ref('poc_fact_monthly') }}
    union all
    select
        series_key,
        date_key
    from {{ ref('poc_fact_daily') }}
) as facts
group by series_key, date_key
having count(*) > 1
