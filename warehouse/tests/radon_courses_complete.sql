-- radon (RAD-on / POL-on, #2990): the newest study-programme copy is the whole register: at least 8,000 programmes
-- (11,696 on 12 Nov 2026), leading institutions in all 16 voivodeships, and no programme without level or status.
-- One test per Silver model (a test reading another source's table would fail that source's first runs).
with checks as (
    select
        count(*) as programmes,
        count(distinct leading_voivodeship) as voivodeships,
        count(*) filter (where level_code is null or status_code is null) as without_level_or_status
    from {{ ref('radon_courses') }}
)

select *
from checks
where programmes < 8000 or voivodeships < 16 or without_level_or_status > 0
