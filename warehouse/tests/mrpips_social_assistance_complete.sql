-- MRPiPS-03 social assistance (warehouse/macros/gus_tables.sql): every report year from the first to the last has
-- each of the three measures for all 17 areas (Poland = RAZEM and the 16 voivodeships). A section, header or
-- label that stops matching in a new year's report fails here.
with counts as (
    select
        measure_code,
        period_year,
        count(distinct area_name) as n
    from {{ ref('mrpips_social_assistance_tables') }}
    group by all
),

years as (
    select unnest(generate_series(min(period_year), max(period_year))) as period_year
    from counts
),

grid as (
    select
        m.measure_code,
        y.period_year
    from years as y
    cross join (values ('persons_granted'), ('families'), ('persons_in_families')) as m (measure_code)
)

select
    g.measure_code,
    g.period_year,
    coalesce(c.n, 0) as found,
    17 as expected
from grid as g
left join counts as c on g.measure_code = c.measure_code and g.period_year = c.period_year
where coalesce(c.n, 0) <> 17
