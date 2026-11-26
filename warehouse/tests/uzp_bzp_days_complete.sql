-- uzp_bzp: every landed (notice type, UTC publication day) holds exactly the number of notices the BZP API's
-- notice/stats gave for it when it was landed, also 0 for a day without notices, and comes from one Landing file.
-- One test per Silver model, reading only that model.
select
    entity,
    notices,
    stats_count,
    stats_count_min,
    files
from {{ ref('uzp_bzp_days') }}
where notices <> stats_count or stats_count_min <> stats_count or files <> 1
