-- NFZ waiting times (PLS-015), newest complete snapshot: every case x voivodeship has queues, and the places
-- confirm the mapping of poc_geography__nfz_waiting_times (NFZ code n = TERYT code 2n): in every NFZ voivodeship
-- the most common TERYT voivodeship of the places is 2n, and places in another voivodeship stay below 1 % of its
-- queues. A branch may contract a provider whose place lies across the border (8 of 148,140 queues in the Dev trial
-- of 13 Nov 2026); the counts are by the NFZ branch that lists the queue, so these few are kept under that branch.
-- Rows returned fail.
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

places as (
    select
        province_code,
        case_code,
        left(teryt_place, 2) as place_voivodeship,
        lpad(cast(cast(province_code as integer) * 2 as varchar), 2, '0') as expected_voivodeship,
        count(*) as n
    from {{ ref('nfz_waiting_times_queues') }}
    where coalesce(teryt_place, '') <> ''
    group by all
),

dominant as (
    select province_code, case_code, place_voivodeship, expected_voivodeship
    from places
    qualify rank() over (partition by province_code, case_code order by n desc) = 1
),

outside as (
    select
        p.province_code,
        p.case_code,
        sum(p.n) filter (where p.place_voivodeship <> p.expected_voivodeship) as n_outside,
        sum(p.n) as n_places
    from places as p
    group by all
)

select g.province_code, g.case_code, 'no queues' as problem, 0 as n
from grid as g
left join counts as c on g.province_code = c.province_code and g.case_code = c.case_code
where c.n is null
union all
select province_code, case_code, 'most places in TERYT voivodeship ' || place_voivodeship, 0
from dominant
where place_voivodeship <> expected_voivodeship
union all
select province_code, case_code, 'places outside the voivodeship above 1 %', n_outside
from outside
where coalesce(n_outside, 0) * 100 >= n_places
