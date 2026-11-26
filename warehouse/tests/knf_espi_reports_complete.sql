-- knf_espi: every day in Silver holds the number of reports the KNF list states for it (listed_for_day; a day of
-- one page states none), and every report sits in the day document of its own day of receipt. A changed pager or
-- row layout that loses rows fails here instead of thinning the counts silently. One test per Silver model.
with days as (
    select
        entity,
        received_date,
        count(*) as found,
        max(listed_for_day) as stated,
        count(distinct listed_for_day) as statements
    from {{ ref('knf_espi_reports') }}
    group by entity, received_date
)

select
    entity,
    received_date,
    found,
    stated,
    'rows differ from the stated count' as problem
from days
where (stated is not null and found <> stated) or statements > 1
union all
select
    entity,
    received_date,
    found,
    stated,
    'report outside its day document' as problem
from days
where entity <> 'day_' || strftime(received_date, '%Y%m%d')
