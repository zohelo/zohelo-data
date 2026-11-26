-- NFZ waiting times (PLS-015): every Bronze entity of a complete month (all 32 entities of 2 cases x 16 voivodeships
-- in Bronze) yields its waiting_lists row in silver.nfz_waiting_times_monthly. A month with fewer entities is left
-- out of Silver on purpose (macro poc_nfz_waiting_times_snapshots) and is not returned here. Rows returned fail.
with names as (
    select entity
    from {{ source('nfz_waiting_times', 'nfz_waiting_times_queues') }}
    group by 1
),

entities as (
    select
        entity,
        regexp_extract(entity, '^queues_([0-9]{6})_case([12])_(0[1-9]|1[0-6])$', ['yyyymm', 'case_no', 'province']) as part
    from names
),

complete as (
    select part.yyyymm as yyyymm
    from entities
    group by 1
    having count(*) = 32
)

select e.entity
from entities as e
inner join complete as c on e.part.yyyymm = c.yyyymm
anti join {{ ref('nfz_waiting_times_monthly') }} as m
    on
        m.measure_code = 'waiting_lists'
        and m.snapshot_month = strptime(e.part.yyyymm || '01', '%Y%m%d')::date
        and m.province_code = e.part.province
        and cast(m.case_code as varchar) = e.part.case_no
