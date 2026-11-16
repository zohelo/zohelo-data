-- zus_yearbook: every measure has all 17 areas (Poland and the 16 voivodeships) in every year between its first and
-- last year, with a value. A title that stops matching in a new edition, a layout whose rows no longer give one value
-- per year (the Silver model then yields nothing for the row) or a lost half of a two-page spread fails here instead
-- of thinning the series silently. One test per Silver model, reading only that model.
with counts as (
    select
        measure_code,
        period_year,
        count(distinct area_name) filter (where cell_status = 'ok') as n
    from {{ ref('zus_yearbook_tables') }}
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
    17 as expected
from periods as p
left join counts as c on p.measure_code = c.measure_code and p.period_year = c.period_year
where coalesce(c.n, 0) <> 17
