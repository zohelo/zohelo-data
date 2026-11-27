-- uokik_decisions (PLG-020, PLG-021): every category in Silver holds what the view states for it: its decisions plus
-- its year categories add up to the category's @descendants, the rows are numbered 1..n without a gap, and Silver is
-- not empty. A changed pager or cell layout that loses decisions fails here instead of thinning the counts silently.
-- One test per Silver model (it reads no other model).
with categories as (
    select
        category_key,
        count(*) as decisions,
        count(distinct year_category) as years,
        max(category_descendants) as stated,
        count(distinct category_descendants) as statements,
        min(source_row_number) as first_row,
        max(source_row_number) as last_row
    from {{ ref('uokik_decisions') }}
    group by category_key
)

select
    category_key,
    decisions || ' decisions + ' || years || ' years, ' || stated || ' stated' as problem
from categories
where decisions + years <> stated or statements > 1
union all
select
    category_key,
    'rows numbered ' || first_row || '..' || last_row || ' for ' || decisions || ' decisions' as problem
from categories
where first_row <> 1 or last_row <> decisions
union all
select
    'all' as category_key,
    'no decisions' as problem
from (select count(*) as n from {{ ref('uokik_decisions') }}) as t
where t.n = 0
