-- sejm (PLG-024): the votings of every sitting are numbered 1..n without a gap and their count equals the number
-- the API's voting days list gives for the sitting (an answer not landed, or rows the decoder dropped, fail here);
-- every voting belongs to the term of its answer. One test per Silver model (it reads no other model).
with votings as (
    select * from {{ ref('sejm_votings') }}
),

per_sitting as (
    select
        term,
        sitting_number,
        count(*) as votings,
        max(voting_number) as last_number,
        min(voting_number) as first_number,
        max(votings_listed) as votings_listed
    from votings
    group by term, sitting_number
)

select
    term,
    sitting_number,
    'voting numbers have a gap' as problem
from per_sitting
where first_number <> 1 or last_number <> votings
union all
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
