-- SIO schools and pupils (models/silver/cie_sio/cie_sio_schools.sql): the 2018 workbook has schools and pupils for
-- all 16 voivodeships (no Poland row is published).
with counts as (
    select
        measure_code,
        count(distinct area_code) as areas
    from {{ ref('cie_sio_schools') }}
    where period_year = 2018
    group by all
)

select
    m.measure_code,
    coalesce(c.areas, 0) as areas,
    16 as expected
from (values ('schools'), ('pupils')) as m (measure_code)
left join counts as c on m.measure_code = c.measure_code
where coalesce(c.areas, 0) <> 16
