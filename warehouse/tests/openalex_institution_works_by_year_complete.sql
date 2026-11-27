-- openalex: counts_by_year of the 200 largest Polish institutions held every year 2010-2026 (Dev probe 45816100460).
-- Fewer than 200 institutions with years, a year outside the twenty years before the read, open-access works above
-- all works, or a negative count fails here. One test per Silver model (it reads only this model).
with y as (select * from {{ ref('openalex_institution_works_by_year') }})

select
    'institutions with yearly counts' as problem,
    count(distinct institution_id) as found
from y
having count(distinct institution_id) < 200
union all
select
    'year outside the twenty years before the read: ' || year as problem,
    count(*) as found
from y
where year not between cast(left(source_version, 4) as integer) - 20 and cast(left(source_version, 4) as integer)
group by year
union all
select
    'open access above all works or a negative count: ' || institution_id || ' ' || year as problem,
    1 as found
from y
where oa_works_count > works_count or works_count < 0 or oa_works_count < 0
