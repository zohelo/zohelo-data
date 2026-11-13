-- ted_notices: every publication day in Silver has all its pages (1 to the last) and every page but the last is
-- full (250 notices, the page size of ingestion/to_landing/ted_notices/). A page lost between Landing and Silver, or
-- an answer cut short, fails here instead of thinning the monthly counts silently. Days without notices have no
-- rows (their empty answers are landed, not loaded as rows). One test per Silver model, reading only that model.
with pages as (
    select
        split_part(entity, '_', 2) as publication_day,
        cast(split_part(entity, '_p', 2) as integer) as page,
        count(*) as notices
    from {{ ref('ted_notices') }}
    group by all
),

days as (
    select
        publication_day,
        max(page) as last_page,
        count(*) as pages
    from pages
    group by publication_day
)

select
    p.publication_day,
    p.page,
    p.notices,
    d.last_page,
    d.pages
from pages as p
inner join days as d on p.publication_day = d.publication_day
where d.pages <> d.last_page or (p.page < d.last_page and p.notices <> 250) or p.notices > 250
