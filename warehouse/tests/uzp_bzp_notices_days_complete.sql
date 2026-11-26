-- uzp_bzp: every (notice type, UTC publication day) in Silver holds exactly the number of notices the BZP API's
-- notice/stats gave for that type and day when it was landed (stats_count, carried from the Landing file), and every
-- notice sits in the file of its own type and day. A notice lost between Landing and Silver, or a file cut short,
-- fails here instead of thinning the monthly counts silently. Days without notices have no rows (their empty answers
-- are landed, not loaded as rows). One test per Silver model, reading only that model.
with days as (
    select
        entity,
        count(*) as notices,
        min(stats_count) as stats_min,
        max(stats_count) as stats_max,
        count(*) filter (
            where entity <> (case notice_type when 'ContractNotice' then 'contract' else 'result' end)
            || '_' || strftime(publication_date, '%Y%m%d')
        ) as misplaced
    from {{ ref('uzp_bzp_notices') }}
    group by entity
)

select *
from days
where notices <> stats_max or stats_min <> stats_max or misplaced > 0
