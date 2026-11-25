-- radon (RAD-on / POL-on, #2990): the newest institutions copy is the whole register, not a cut-off answer: at least
-- 500 institutions (840 on 12 Nov 2026) and operating institutions in all 16 voivodeships. Landing already refuses
-- a resource whose records differ from the API's maxCount; this catches a layout change that empties the columns.
-- One test per Silver model (a test reading another source's table would fail that source's first runs).
with checks as (
    select
        count(*) as institutions,
        count(distinct voivodeship) filter (where status_name = 'Działająca') as voivodeships
    from {{ ref('radon_institutions') }}
)

select *
from checks
where institutions < 500 or voivodeships < 16
