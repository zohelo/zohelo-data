-- gus_exam_results: the CSV is a full grid. Every year from 2019 to the newest one has every area x subject x sex
-- (17 areas x 8 subjects x 3 sex groups = 408 rows), and Poland ('Ogółem', sex ogółem) has a value for Polish and
-- mathematics, the subjects every pupil sits. A lost year, area, subject or sex group, or a sign where the national
-- mean of a compulsory subject should be, fails here instead of thinning the series silently. One test per Silver model.
with years as (
    select unnest(generate_series(2019, (select max(period_year) from {{ ref('gus_exam_results_eighth_grade') }}))) as period_year
),

counts as (
    select
        period_year,
        count(*) as n,
        count(distinct area_name) as areas,
        count(distinct subject_code) as subjects,
        count(distinct sex_code) as sexes,
        count(*) filter (
            where area_name = 'Ogółem' and sex_code = 'T' and subject_code in ('polish', 'mathematics') and value is not null
        ) as national_compulsory
    from {{ ref('gus_exam_results_eighth_grade') }}
    group by all
)

select
    y.period_year,
    coalesce(c.n, 0) as found_rows,
    408 as expected_rows,
    c.areas,
    c.subjects,
    c.sexes,
    c.national_compulsory
from years as y
left join counts as c on y.period_year = c.period_year
where coalesce(c.n, 0) <> 408 or c.areas <> 17 or c.subjects <> 8 or c.sexes <> 3 or c.national_compulsory <> 2
