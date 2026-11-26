-- sejm (PLG-024): the number of votings of every sitting with votings rows equals the number the API's voting days
-- list gives for the sitting (rows the decoder dropped fail here; a sitting of that list without any votings row,
-- whose answer was not landed or loaded, fails sejm_sitting_days_complete), and every voting belongs to the term of
-- its answer. Voting numbers are not checked for gaps: in term 9 the API skips numbers in 10 sittings while the
-- counts match its own list (Dev trial). One test per Silver model (it reads no other model).
with votings as (
    select * from {{ ref('sejm_votings') }}
),

per_sitting as (
    select
        term,
        sitting_number,
        count(*) as votings,
        max(votings_listed) as votings_listed
    from votings
    group by term, sitting_number
)

select
    term,
    sitting_number,
    'count differs from the voting days list' as problem
from per_sitting
where votings_listed is null or votings <> votings_listed
union all
select
    term,
    sitting_number,
    'item of another term' as problem
from votings
where item_term <> term
union all
select
    null as term,
    null as sitting_number,
    'no votings' as problem
where not exists (select 1 from votings)
