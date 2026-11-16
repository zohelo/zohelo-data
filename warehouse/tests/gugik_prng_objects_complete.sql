-- GUGiK PRNG (silver.prng_objects): both parts of the national register are present and neither is cut short. The
-- packages of 12 Nov 2026 held 124,236 localities and 131,525 physiographic objects; a part with fewer than 100,000
-- objects (a truncated or wrong file that still passed the Bronze layout checks) fails here.
with expected (entity) as (
    values ('miejscowosci'), ('obiekty_fizjograficzne')
),

counts as (
    select
        entity,
        count(*) as n
    from {{ ref('prng_objects') }}
    group by entity
)

select
    e.entity,
    coalesce(c.n, 0) as objects
from expected as e
left join counts as c on e.entity = c.entity
where coalesce(c.n, 0) < 100000
