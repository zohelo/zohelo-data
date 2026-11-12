-- GUS PSR 2020 census tables (warehouse/macros/gus_tables.sql): the one-off workbook must give exactly the curated
-- measure x period combinations (6, 102 rows), each with all 17 areas (Poland and the 16
-- voivodeships). A title, header or block that stops matching, or a layout that loses the voivodeship rows, fails
-- here. One test per Silver model (a test reading another source's table would fail that source's first runs).
with expected (measure_code, period_year) as (
    values
    ('farms', 2010),
    ('farms', 2020),
    ('agricultural_land', 2020),
    ('average_agricultural_land', 2020),
    ('cattle', 2020),
    ('pigs', 2020)
),

counts as (
    select
        measure_code,
        period_year,
        count(distinct area_name) as n
    from {{ ref('gus_psr2020_tables') }}
    group by all
)

select
    coalesce(e.measure_code, c.measure_code) as measure_code,
    coalesce(e.period_year, c.period_year) as period_year,
    coalesce(c.n, 0) as found,
    case when e.measure_code is null then 0 else 17 end as expected
from expected as e
full outer join counts as c on e.measure_code = c.measure_code and e.period_year = c.period_year
where coalesce(c.n, 0) <> case when e.measure_code is null then 0 else 17 end
