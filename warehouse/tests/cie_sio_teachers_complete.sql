-- SIO teachers (models/silver/cie_sio/cie_sio_teachers.sql): Poland has 4 measures x 5 grades for every year from
-- 2006 (the first in the workbook) to the last, and the voivodeship block has the same 20 cells for all 16
-- voivodeships in exactly the last year. A header, label or block that stops matching fails here.
with counts as (
    select
        period_year,
        area_code = 'PL' as is_poland,
        count(distinct area_code) as areas,
        count(*) as n
    from {{ ref('cie_sio_teachers') }}
    group by all
),

last_year as (
    select max(period_year) as y
    from counts
    where is_poland
),

expected as (
    select
        unnest(generate_series(2006, y)) as period_year,
        true as is_poland,
        1 as areas,
        20 as n
    from last_year
    union all
    select
        y as period_year,
        false as is_poland,
        16 as areas,
        320 as n
    from last_year
)

select
    coalesce(e.period_year, c.period_year) as period_year,
    coalesce(e.is_poland, c.is_poland) as is_poland,
    c.areas,
    c.n,
    e.areas as expected_areas,
    e.n as expected_rows
from expected as e
full join counts as c on e.period_year = c.period_year and e.is_poland = c.is_poland
where c.n is distinct from e.n or c.areas is distinct from e.areas
