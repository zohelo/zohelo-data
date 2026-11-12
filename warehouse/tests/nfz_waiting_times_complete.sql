-- NFZ waiting times (PLS-015), newest complete snapshot: every case x voivodeship has queues, and every place's
-- TERYT code lies in the voivodeship it was asked under (NFZ code n = TERYT code 2n, the mapping of
-- poc_geography__nfz_waiting_times). Rows returned fail.
with grid as (
    select
        lpad(cast(p as varchar), 2, '0') as province_code,
        c as case_code
    from range(1, 17) as t1 (p)
    cross join range(1, 3) as t2 (c)
),

counts as (
    select province_code, case_code, count(*) as n
    from {{ ref('nfz_waiting_times_queues') }}
    group by all
),

teryt as (
    select province_code, case_code, count(*) as n
    from {{ ref('nfz_waiting_times_queues') }}
    where coalesce(teryt_place, '') <> ''
        and left(teryt_place, 2) <> lpad(cast(cast(province_code as integer) * 2 as varchar), 2, '0')
    group by all
)

select g.province_code, g.case_code, 'no queues' as problem, 0 as n
from grid as g
left join counts as c on g.province_code = c.province_code and g.case_code = c.case_code
where c.n is null
union all
select province_code, case_code, 'place outside the voivodeship (TERYT)', n
from teryt
