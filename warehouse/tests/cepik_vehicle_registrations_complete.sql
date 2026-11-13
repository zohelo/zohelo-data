-- cepik: every month between the first and the last published month has all 17 areas (16 voivodeships and the
-- national total 'polska') in the voivodeship table and at least one vehicle type in the type table. A header or
-- month name that stops matching in a new workbook, a lost national row or a missing workbook fails here instead of
-- thinning the series silently. One test per Silver model (a test reading another source's table would fail that
-- source's first runs).
with counts as (
    select
        period_date,
        count(distinct case when table_code = 'voivodeship' then group_code end) as areas,
        count(distinct case when table_code = 'vehicle_type' then group_code end) as types
    from {{ ref('cepik_vehicle_registrations') }}
    group by all
),

months as (
    select cast(unnest(generate_series(min(period_date), max(period_date), interval 1 month)) as date) as period_date
    from counts
)

select
    m.period_date,
    coalesce(c.areas, 0) as areas_found,
    17 as areas_expected,
    coalesce(c.types, 0) as types_found
from months as m
left join counts as c on m.period_date = c.period_date
where coalesce(c.areas, 0) <> 17 or coalesce(c.types, 0) = 0
