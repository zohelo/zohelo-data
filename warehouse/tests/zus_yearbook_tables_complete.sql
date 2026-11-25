-- zus_yearbook: every measure has all 17 areas (Poland and the 16 voivodeships) in every year between its first and
-- last year, with a value, and the 16 voivodeships add up to between 95 % and 100 % of Poland (exactly 100 % in the
-- 2009-2011 edition, 96.9-99.6 % from 2012: the tables leave some payers and persons in no voivodeship; Dev trial
-- 45708846339). A title that stops matching in a new edition, a row that no longer gives one value per year (the
-- Silver model then yields nothing for it), a lost half of a two-page spread or a value read from the wrong column
-- or with a lost digit group fails here instead of thinning or bending the series. One test per Silver model,
-- reading only that model.
with t as (
    select * from {{ ref('zus_yearbook_tables') }}
),

counts as (
    select
        measure_code,
        period_year,
        count(distinct area_name) filter (where cell_status = 'ok') as n,
        sum(value) filter (where area_name = 'polska') as poland,
        sum(value) filter (where area_name <> 'polska') as voivodeships
    from t
    group by all
),

periods as (
    select
        measure_code,
        unnest(generate_series(min(period_year), max(period_year))) as period_year
    from counts
    group by measure_code
)

select
    p.measure_code,
    p.period_year,
    coalesce(c.n, 0) as found,
    17 as expected,
    c.poland,
    c.voivodeships
from periods as p
left join counts as c on p.measure_code = c.measure_code and p.period_year = c.period_year
where coalesce(c.n, 0) <> 17 or not coalesce(c.voivodeships between 0.95 * c.poland and c.poland, false)
