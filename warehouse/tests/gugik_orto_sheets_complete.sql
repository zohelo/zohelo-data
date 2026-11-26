-- GUGiK orthophoto sheet index (silver.gugik_orto_sheets, written per year by the loader): every year from 2009 to
-- 2025 is present and none is cut short. On 26 Nov 2026 each of those years held 17,800 to 147,937 index features
-- (resultType=hits, Dev probe 45810994708); a year with fewer than 10,000 (a lost or truncated year) fails here.
with expected as (
    select unnest(range(2009, 2026)) as data_year
),

counts as (
    select
        data_year,
        count(*) as n
    from {{ source('gugik_orto_entities', 'gugik_orto_sheets') }}
    group by data_year
)

select
    e.data_year,
    coalesce(c.n, 0) as features
from expected as e
left join counts as c on e.data_year = c.data_year
where coalesce(c.n, 0) < 10000
