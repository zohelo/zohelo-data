-- GUNB RWDZ build stage: every month has the total for Poland and all 16 voivodeships, and every month between the
-- first and the last is present. A voivodeship file that stops loading (rejected, not landed) or a month the register
-- leaves empty would otherwise thin the series silently. Returns the months with another count. Reads only the
-- source's own model (a test reading another source's table would fail this source's first runs).
with counts as (
    select
        date_key,
        count(distinct geography_key) filter (where geography_key = 'PL' or geography_key like 'TERC:__') as n
    from {{ ref('poc_build_gunb_rwdz') }}
    where is_total
    group by date_key
),

months as (
    select unnest(generate_series(min(date_key), max(date_key), interval 1 month)) as date_key
    from counts
)

select
    cast(m.date_key as date) as date_key,
    coalesce(c.n, 0) as found,
    17 as expected
from months as m
left join counts as c on cast(m.date_key as date) = c.date_key
where coalesce(c.n, 0) <> 17
