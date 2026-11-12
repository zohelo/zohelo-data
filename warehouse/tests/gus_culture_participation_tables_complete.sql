-- gus_culture_participation (warehouse/macros/gus_tables.sql): every period x measure of the Silver model has all
-- three sex groups (survey editions). A table whose title or header stops matching in a new edition, or a layout
-- that loses the voivodeship rows, fails here instead of thinning the series silently. One test per Silver model (a
-- test reading another source's table would fail that source's first runs).
with counts as (
    select
        measure_code,
        period_year,
        count(distinct group_code) as n
    from {{ ref('gus_culture_participation_tables') }}
    group by all
),

periods as (
    select distinct period_year from counts
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
    3 as expected
from grid as g
left join counts as c on g.measure_code = c.measure_code and g.period_year = c.period_year
where coalesce(c.n, 0) <> 3
