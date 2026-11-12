-- GUS publication tables (warehouse/macros/gus_tables.sql): every period x measure of each Silver model has all its
-- areas (Poland and the 16 voivodeships) or, for the survey, all three sex groups; the annual series have every
-- year between their first and last edition. A table whose title or header stops matching in a new edition, or a
-- layout that loses the voivodeship rows, fails here instead of thinning the series silently.
with counts as (
    select
        'gus_culture' as model,
        measure_code,
        period_year,
        count(distinct area_name) as n,
        17 as expected
    from {{ ref('gus_culture_tables') }}
    group by all
    union all
    select
        'gus_tourism',
        measure_code,
        period_year,
        count(distinct area_name),
        17
    from {{ ref('gus_tourism_tables') }}
    group by all
    union all
    select
        'gus_culture_participation',
        measure_code,
        period_year,
        count(distinct group_code),
        3
    from {{ ref('gus_culture_participation_tables') }}
    group by all
),

annual as (
    select
        model,
        min(period_year) as first_year,
        max(period_year) as last_year
    from counts
    where model in ('gus_culture', 'gus_tourism')
    group by all
),

periods as (
    select distinct
        model,
        period_year
    from counts
    union
    select
        model,
        unnest(generate_series(first_year, last_year)) as period_year
    from annual
),

measures as (
    select distinct
        model,
        measure_code,
        expected
    from counts
),

grid as (
    select
        p.model,
        m.measure_code,
        p.period_year,
        m.expected
    from periods as p
    inner join measures as m on p.model = m.model
)

select
    g.model,
    g.measure_code,
    g.period_year,
    coalesce(c.n, 0) as found,
    g.expected
from grid as g
left join counts as c
    on g.model = c.model and g.measure_code = c.measure_code and g.period_year = c.period_year
where coalesce(c.n, 0) <> g.expected
