-- Crossref works with a Polish affiliation: every year from 2000 (FIRST_YEAR in ingestion/to_landing/crossref/) to
-- the newest has its own request with a total and a type breakdown, and that total agrees within 1% with the
-- year's count in the all-years request (the two are read seconds apart, so deposits indexed in between may differ).
with counts as (select * from {{ ref('crossref_works_pl_counts') }}),

years as (
    select unnest(generate_series(2000, max(publication_year))) as publication_year
    from counts
    where entity <> 'works_pl_by_year'
),

yearly as (
    select
        publication_year,
        max(case when facet = '_total' then works end) as total,
        count(*) filter (where facet = 'type-name') as types
    from counts
    where entity <> 'works_pl_by_year'
    group by all
),

overall as (
    select
        publication_year,
        works
    from counts
    where entity = 'works_pl_by_year' and facet = 'published'
)

select
    y.publication_year,
    t.total,
    t.types,
    o.works as all_years_count
from years as y
left join yearly as t on y.publication_year = t.publication_year
left join overall as o on y.publication_year = o.publication_year
where t.total is null
    or (t.total > 0 and t.types = 0)
    or abs(t.total - coalesce(o.works, 0)) > 0.01 * greatest(t.total, coalesce(o.works, 0))
