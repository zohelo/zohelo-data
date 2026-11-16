-- ted_notices: every publication day in Silver has all its pages (1 to the last), every page but the last is full
-- (250 notices, the page size of ingestion/to_landing/ted_notices/), and the day's notices equal the total its
-- answers state (day_total_notices, every page of the day the same). A page lost between Landing and Silver, a
-- day landed only in part (also its last pages), or an answer cut short fails here instead of thinning the
-- monthly counts silently. Days without notices have no rows (their empty answers are landed, not loaded as
-- rows); a day without any landed page is missing in landing.inspect. One test per Silver model.
with pages as (
    select
        split_part(entity, '_', 2) as publication_day,
        cast(split_part(entity, '_p', 2) as integer) as page,
        count(*) as notices,
        min(day_total_notices) as total_min,
        max(day_total_notices) as total_max
    from {{ ref('ted_notices') }}
    group by all
),

days as (
    select
        publication_day,
        max(page) as last_page,
        count(*) as pages,
        sum(notices) as notices,
        min(total_min) as total_min,
        max(total_max) as total_max
    from pages
    group by publication_day
)

select
    p.publication_day,
    p.page,
    p.notices as page_notices,
    d.last_page,
    d.pages,
    d.notices as day_notices,
    d.total_min,
    d.total_max
from pages as p
inner join days as d on p.publication_day = d.publication_day
where
    d.pages <> d.last_page
    or (p.page < d.last_page and p.notices <> 250)
    or p.notices > 250
    or d.total_min <> d.total_max
    or d.notices <> d.total_max
