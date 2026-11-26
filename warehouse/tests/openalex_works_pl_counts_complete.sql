-- openalex: the works_pl copy held 175 publication years for all works and 26 fields (Dev probe 45813299261, 26 Nov
-- 2026). A copy cut short (fewer than 150 years, a field without rows, no year as late as the year before the read),
-- or counts that contradict each other (open access or the sum of the fields above all works in a year) fails here
-- instead of thinning Gold silently. One test per Silver model (it reads only this model).
with c as (select * from {{ ref('openalex_works_pl_counts') }})

select
    'years of all works' as problem,
    count(*) as found
from c
where selection = 'all'
having count(*) < 150
union all
select
    'fields with rows' as problem,
    count(distinct field_id) as found
from c
where selection = 'field'
having count(distinct field_id) <> 26
union all
select
    'latest year of all works before the year of the read less one' as problem,
    max(publication_year) as found
from c
where selection = 'all'
having max(publication_year) < max(cast(left(source_version, 4) as integer)) - 1
union all
select
    'open access or fields above all works in ' || a.publication_year as problem,
    a.works as found
from c as a
left join c as o on o.selection = 'open_access' and a.publication_year = o.publication_year
left join (
    select
        publication_year,
        sum(works) as works
    from c
    where selection = 'field'
    group by publication_year
) as f on a.publication_year = f.publication_year
where a.selection = 'all' and (coalesce(o.works, 0) > a.works or coalesce(f.works, 0) > a.works)
