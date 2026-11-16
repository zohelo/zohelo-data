-- SIO graduates (models/silver/cie_sio/cie_sio_graduates.sql): every reference year 2010-2018 (nine workbooks) has
-- graduates (sex T) for Poland and the 16 voivodeships in each of the three pupil categories.
with counts as (
    select
        period_year,
        pupil_category_code,
        count(distinct area_code) as areas
    from {{ ref('cie_sio_graduates') }}
    where sex_code = 'T'
    group by all
),

grid as (
    select
        y.period_year,
        k.pupil_category_code
    from (select unnest(generate_series(2010, 2018)) as period_year) as y
    cross join (values ('001'), ('002'), ('003')) as k (pupil_category_code)
)

select
    g.period_year,
    g.pupil_category_code,
    coalesce(c.areas, 0) as areas,
    17 as expected
from grid as g
left join counts as c on g.period_year = c.period_year and g.pupil_category_code = c.pupil_category_code
where coalesce(c.areas, 0) <> 17
