-- gus_health (warehouse/macros/gus_tables.sql): every period x measure of the Silver model has all 17 areas
-- (Poland and the 16 voivodeships), and every measure has every year from its first to its last edition; the
-- six measures together cover every year from the first edition of the tables (2012) to the newest one. A table
-- whose title or header stops matching in a new edition, a lost edition or a layout that loses the Poland or a
-- voivodeship row fails here instead of thinning the series silently. One test per Silver model.
with counts as (
    select
        measure_code,
        period_year,
        count(distinct area_name) as n
    from {{ ref('gus_health_tables') }}
    group by all
),

measures as (
    select
        measure_code,
        min(period_year) as first_year,
        max(period_year) as last_year
    from counts
    group by all
),

grid as (
    select
        m.measure_code,
        unnest(generate_series(m.first_year, m.last_year)) as period_year
    from measures as m
),

years as (
    select unnest(generate_series(2012, (select max(period_year) from counts))) as period_year
),

missing_years as (
    select
        '(any)' as measure_code,
        y.period_year,
        0 as found,
        17 as expected
    from years as y
    where y.period_year not in (select c.period_year from counts as c)
)

select
    g.measure_code,
    g.period_year,
    coalesce(c.n, 0) as found,
    17 as expected
from grid as g
left join counts as c on g.measure_code = c.measure_code and g.period_year = c.period_year
where coalesce(c.n, 0) <> 17
union all
select * from missing_years
