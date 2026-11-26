-- sejm (PLG-024): the prints of each term match the count /sejm/term lists (up to 3 apart for the current term,
-- whose lists are read some seconds apart while prints are added), and every print belongs to its answer's term.
-- One test per Silver model (it reads no other model).
with prints as (
    select * from {{ ref('sejm_prints') }}
),

per_term as (
    select
        term,
        count(*) as prints,
        max(prints_listed) as prints_listed
    from prints
    group by term
)

select
    term,
    'count differs from /sejm/term' as problem
from per_term
where prints_listed is null or abs(prints - prints_listed) > 3
union all
select
    term,
    'item of another term' as problem
from prints
where item_term <> term
union all
select
    t.term,
    'term has no prints' as problem
from (values (9), (10)) as t (term)
where not exists (select 1 from prints as p where p.term = t.term)
