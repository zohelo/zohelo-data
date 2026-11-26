-- NFZ waiting times (PLS-015) monthly counts: every snapshot month has the queue entries of Poland ('00') and the
-- 16 voivodeships for both cases, and Poland is the sum of the voivodeships for every measure and month.
with lists as (
    select snapshot_month, count(*) as n
    from {{ ref('nfz_waiting_times_monthly') }}
    where measure_code = 'waiting_lists'
    group by all
),

sums as (
    select
        measure_code,
        case_code,
        period_month,
        sum(value) filter (where province_code = '00') as poland,
        sum(value) filter (where province_code <> '00') as voivodeships
    from {{ ref('nfz_waiting_times_monthly') }}
    group by all
)

select cast(snapshot_month as varchar) as what, 'areas x cases' as problem, n
from lists
where n <> 34
union all
select measure_code || ' case ' || case_code || ' ' || period_month, 'Poland differs from the sum', poland - voivodeships
from sums
where poland is distinct from voivodeships
