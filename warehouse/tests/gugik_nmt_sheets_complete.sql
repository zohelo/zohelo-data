-- GUGiK elevation sheet indexes (silver.gugik_nmt_sheets, written per service's year by the loader): all six services
-- are present with their years and none is cut short. On 26 Nov 2026 each service listed 8 to 17 years and 81,265 to
-- 447,892 index features (Dev trial 45873287115); a service with fewer than 8 years or 50,000
-- features (a lost or truncated service) fails here.
with expected (service) as (
    values ('nmt_evrf2007'), ('nmt_kron86'), ('nmpt_evrf2007'), ('nmpt_kron86'), ('lidar_evrf2007'), ('lidar_kron86')
),

counts as (
    select
        product || '_' || service_datum as service,
        count(distinct data_year) as years,
        count(*) as n
    from {{ source('gugik_nmt_entities', 'gugik_nmt_sheets') }}
    group by 1
)

select
    e.service,
    coalesce(c.years, 0) as years,
    coalesce(c.n, 0) as features
from expected as e
left join counts as c on e.service = c.service
where coalesce(c.years, 0) < 8 or coalesce(c.n, 0) < 50000
