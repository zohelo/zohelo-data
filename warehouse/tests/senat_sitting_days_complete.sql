-- senat (PLG-025): every sitting of term XI from 1 to the newest finished one has its days, and within a sitting the
-- days follow each other in date order: a list page that was not read, a sitting dropped by the parser or a date
-- read in the wrong order fails here. One test per Silver model (it reads no other model).
with days as (
    select * from {{ ref('senat_sitting_days') }}
),

sittings as (
    select distinct sitting_number from days
),

expected as (
    select unnest(range(1, (select max(sitting_number) from days where is_finished) + 1)) as sitting_number
)

select
    e.sitting_number,
    null as day_number,
    'sitting missing' as problem
from expected as e
left join sittings as s on e.sitting_number = s.sitting_number
where s.sitting_number is null
union all
select
    sitting_number,
    day_number,
    'day dates out of order' as problem
from (
    select
        sitting_number,
        day_number,
        day_date,
        lag(day_date) over (partition by sitting_id order by day_number) as previous_date
    from days
) as ordered
where day_date <= previous_date
union all
select
    null as sitting_number,
    null as day_number,
    'no sitting days' as problem
where not exists (select 1 from days)
