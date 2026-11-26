-- kprm_work_list (PLG-034): every row of the newest work-list file is in Silver (source_row_number 1..n without a gap,
-- so a dropped row fails here) and the list is not empty. One test per Silver model (it reads no other model).
select
    'source rows ' || count(*) || ', numbered ' || coalesce(min(source_row_number), 0) || '..'
    || coalesce(max(source_row_number), 0) as problem
from {{ ref('kprm_work_list_drafts') }}
having count(*) = 0 or min(source_row_number) <> 1 or max(source_row_number) <> count(*)
