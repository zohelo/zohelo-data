-- knf_entities (PLG-016): every document of the newest ESMA register answer is in Silver (source_row_number 1..n
-- without a gap, so a dropped row fails here) and the slice is not empty. One test per Silver model (it reads no
-- other model).
select
    'source rows ' || count(*) || ', numbered ' || coalesce(min(source_row_number), 0) || '..'
    || coalesce(max(source_row_number), 0) as problem
from {{ ref('knf_entities_esma_entries') }}
having count(*) = 0 or min(source_row_number) <> 1 or max(source_row_number) <> count(*)
