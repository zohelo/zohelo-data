-- policja_statistics: every category workbook (ogolem, wybrane_7) is present; every year between its first and last
-- year is present; every category x year x measure has Poland and all 16 voivodeships ("woj. ...") and as many areas
-- as the category's fullest year (the two police areas inside Mazowieckie included). A header that stops matching, a
-- category page that drops its workbook or a layout that loses rows fails here instead of thinning the series
-- silently. One test per Silver model (refs only this model).
with counts as (
    select
        category_code,
        measure_code,
        period_year,
        count(distinct area_name) as n,
        count(distinct area_name) filter (where area_name = 'Polska' or area_name like 'woj. %') as n_official
    from {{ ref('policja_statistics_offences') }}
    group by all
),

categories as (
    select * from (values ('ogolem'), ('wybrane_7')) as t (category_code)
),

spans as (
    select
        category_code,
        min(period_year) as first_year,
        max(period_year) as last_year,
        max(n) as max_n
    from counts
    group by all
),

grid as (
    select
        c.category_code,
        m.measure_code,
        unnest(generate_series(coalesce(s.first_year, 0), coalesce(s.last_year, 0))) as period_year,
        s.max_n
    from categories as c
    left join spans as s on c.category_code = s.category_code
    cross join (values ('recorded'), ('detected'), ('detection_rate')) as m (measure_code)
)

select
    g.category_code,
    g.measure_code,
    g.period_year,
    coalesce(c.n, 0) as found,
    coalesce(c.n_official, 0) as found_poland_and_voivodeships,
    g.max_n as expected
from grid as g
left join counts as c
    on g.category_code = c.category_code and g.measure_code = c.measure_code and g.period_year = c.period_year
where g.max_n is null or coalesce(c.n, 0) <> g.max_n or coalesce(c.n_official, 0) <> 17
