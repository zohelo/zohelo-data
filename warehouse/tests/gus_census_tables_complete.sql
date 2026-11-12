-- GUS census tables (warehouse/macros/gus_tables.sql): the one-off census workbooks must give exactly the curated
-- measure x period combinations, each with all 17 areas (Poland and the 16 voivodeships): NSP 2021 8 combinations
-- (136 rows), PSR 2020 6 (102 rows). A title, header or block that stops matching, or a layout that loses the
-- voivodeship rows, fails here.
with expected (model, measure_code, period_year) as (
    values
    ('gus_nsp2021', 'population', 2011),
    ('gus_nsp2021', 'population', 2021),
    ('gus_nsp2021', 'population_urban', 2021),
    ('gus_nsp2021', 'population_rural', 2021),
    ('gus_nsp2021', 'urban_share', 2021),
    ('gus_nsp2021', 'activity_rate', 2021),
    ('gus_nsp2021', 'employment_rate', 2021),
    ('gus_nsp2021', 'unemployment_rate', 2021),
    ('gus_psr2020', 'farms', 2010),
    ('gus_psr2020', 'farms', 2020),
    ('gus_psr2020', 'agricultural_land', 2020),
    ('gus_psr2020', 'average_agricultural_land', 2020),
    ('gus_psr2020', 'cattle', 2020),
    ('gus_psr2020', 'pigs', 2020)
),

counts as (
    select
        'gus_nsp2021' as model,
        measure_code,
        period_year,
        count(distinct area_name) as n
    from {{ ref('gus_nsp2021_tables') }}
    group by all
    union all
    select
        'gus_psr2020',
        measure_code,
        period_year,
        count(distinct area_name)
    from {{ ref('gus_psr2020_tables') }}
    group by all
)

select
    coalesce(e.model, c.model) as model,
    coalesce(e.measure_code, c.measure_code) as measure_code,
    coalesce(e.period_year, c.period_year) as period_year,
    coalesce(c.n, 0) as found,
    case when e.model is null then 0 else 17 end as expected
from expected as e
full outer join counts as c
    on e.model = c.model and e.measure_code = c.measure_code and e.period_year = c.period_year
where coalesce(c.n, 0) <> case when e.model is null then 0 else 17 end
