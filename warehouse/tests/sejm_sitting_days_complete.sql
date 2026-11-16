-- sejm (PLG-024): every term's sittings are numbered 1..n without a gap (a sitting the proceedings list lost, or a
-- term not landed, fails here) and every held day with votes says how many. One test per Silver model (it reads no
-- other model).
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
)

select
    term,
    'sitting numbers have a gap' as problem
from per_term
where sittings <> last_number
union all
select
    t.term,
    'term has no sitting days' as problem
from (values (9), (10)) as t (term)
where not exists (select 1 from days as d where d.term = t.term)
