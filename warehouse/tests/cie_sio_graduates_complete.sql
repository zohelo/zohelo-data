-- SIO graduates (models/silver/cie_sio/cie_sio_graduates.sql): every reference year 2010-2018 (nine workbooks) has
-- graduates (sex T) in each of the three pupil categories for the 16 voivodeships and, 2010-2017, for Poland (the
-- 2018 workbook prints Poland only as one grand total, pupil_category_code '_T').
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
    case when g.period_year = 2018 then 16 else 17 end as expected
from grid as g
left join counts as c on g.period_year = c.period_year and g.pupil_category_code = c.pupil_category_code
where coalesce(c.areas, 0) <> case when g.period_year = 2018 then 16 else 17 end
