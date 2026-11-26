-- sejm (PLG-024): every term's sittings are numbered 1..n without a gap (a sitting the proceedings list lost, or a
-- term not landed, fails here); every day the voting days list gives for a sitting is one of its days with its
-- count (votings_listed), and is held; and every sitting's votings rows in Bronze number what the voting days list
-- gives (a votings answer not landed, rejected or cut off fails here, also for a sitting without any votings row).
-- One test per Silver model (it reads no other model).
with days as (
    select * from {{ ref('sejm_sitting_days') }}
),

per_term as (
    select
        term,
        count(distinct sitting_number) as sittings,
        max(sitting_number) as last_number
    from days
    group by term
),

per_sitting as (
    select
        term,
        sitting_number,
        count(*) filter (where votings_listed is not null) as voting_days_found,
        max(sitting_voting_days_listed) as voting_days_listed,
        max(sitting_votings_listed) as votings_listed,
        max(sitting_votings_loaded) as votings_loaded
    from days
    group by term, sitting_number
)

select
    term,
    null as sitting_number,
    'sitting numbers have a gap' as problem
from per_term
where sittings <> last_number
union all
select
    t.term,
    null as sitting_number,
    'term has no sitting days' as problem
from (values (9), (10)) as t (term)
where not exists (select 1 from days as d where d.term = t.term)
union all
select
    term,
    sitting_number,
    'a day with votes is not a day of the sitting' as problem
from per_sitting
where voting_days_found <> voting_days_listed
union all
select
    term,
    sitting_number,
    'votings loaded differ from the voting days list' as problem
from per_sitting
where votings_loaded <> votings_listed
union all
select
    term,
    sitting_number,
    'a day with votes is not held' as problem
from days
where votings_listed is not null and not is_held
