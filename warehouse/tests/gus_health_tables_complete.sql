-- gus_health (warehouse/macros/gus_tables.sql): every period x measure of the Silver model has all 17 areas
-- (Poland and the 16 voivodeships); every year between the first and the last edition is present for every
-- measure. A table whose title or header stops matching in a new edition, or a layout that loses the voivodeship
-- rows, fails here instead of thinning the series silently. One test per Silver model.
with counts as (
    select
        measure_code,
        period_year,
        count(distinct area_name) as n
    from {{ ref('gus_health_tables') }}
    group by all
),

periods as (
    select distinct period_year from counts
    union
    select unnest(generate_series(min(period_year), max(period_year))) as period_year
    from counts
),

measures as (
    select distinct measure_code from counts
),

grid as (
    select
        m.measure_code,
        p.period_year
    from periods as p
    cross join measures as m
)

select
    g.measure_code,
    g.period_year,
    coalesce(c.n, 0) as found,
    17 as expected
from grid as g
left join counts as c on g.measure_code = c.measure_code and g.period_year = c.period_year
where coalesce(c.n, 0) <> 17
