-- Gold metrics: every value lies within a plausible range for its KPI and source. The bounds catch
-- unit and scale errors (a value ×1000, a percentage stored as a share), not small mistakes. Change
-- a bound only with the publisher's own value as evidence, cited in a comment next to it.
-- A KPI × source without a bound fails, so a new source gets one here.
with bounds (concept_key, source_system, value_min, value_max) as (
    values
    ('population', 'gus_bdl', 0, 5e7),
    ('population', 'gus_dbw', 0, 5e7),
    ('gdp_current_prices', 'eurostat', 0, 3e7),
    ('gdp_current_prices', 'world_bank_wdi', 0, 3e14),
    ('inflation', 'eurostat', -100, 3000),
    ('inflation', 'world_bank_wdi', -100, 100000),
    ('unemployment_rate', 'eurostat', 0, 60),
    ('unemployment_rate', 'world_bank_wdi', 0, 60),
    ('fx_rate', 'nbp', 0, 100000),
    ('gdp_per_capita', 'eurostat', 0, 2e5),
    ('gdp_per_capita', 'world_bank_wdi', 0, 5e5),
    ('gdp_volume_growth', 'eurostat', -50, 100),
    ('gdp_volume_growth', 'world_bank_wdi', -100, 200),
    -- slice needs-block rows (#2743): every source of the concepts unioned below
    ('core_inflation', 'eurostat', -10, 600),  -- 550.6 (Dev trial 43093114523: a candidate country in the 1990s)
    ('min_wage', 'eurostat', 1, 1e6),  -- national currencies; 322,800 (Dev trial 43093114523)
    ('adult_learning', 'eurostat', 0, 100),
    ('adult_learning', 'gus_bdl', 0, 100),
    ('extreme_poverty', 'gus_bdl', 0, 100),
    ('industrial_production', 'eurostat', -100, 300),  -- % change on a year earlier
    ('industrial_production', 'gus_bdl', 1, 300),  -- index, previous year = 100
    ('industrial_production', 'gus_dbw', 1, 300),  -- index, same month a year earlier = 100
    ('construction_output', 'eurostat', -100, 400),  -- % change on a year earlier; 261 (Dev trial 43093114523)
    ('construction_output', 'gus_bdl', 1, 300),  -- index, previous year = 100
    ('construction_output', 'gus_dbw', 1, 300),  -- index, same month a year earlier = 100
    ('employment_enterprise', 'gus_bdl', 1000, 2e7),  -- persons; Poland 6,702,934 in 2026-08
    ('median_wage', 'gus_bdl', 500, 1e5),  -- PLN; Poland 7,530.45 in 2026-03
    ('median_wage', 'gus_dbw', 500, 1e5),
    ('price_m2_secondary', 'gus_bdl', 300, 1e5),  -- PLN per m2; 2,202 the lowest (Dev trial 43093114523)
    ('price_m2_secondary', 'gus_dbw', 300, 1e5),
    ('arope_rate', 'eurostat', 0, 100),
    ('arope_rate', 'gus_dbw', 0, 100),
    ('long_term_unemployment', 'eurostat', 0, 100),
    ('long_term_unemployment', 'gus_dbw', 0, 100),
    ('registered_unemployed', 'gus_bdl', 0, 5e6),
    ('registered_unemployed', 'gus_dbw', 0, 5e6)
),

metrics as (
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_population') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_gdp_current_prices') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_inflation') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_unemployment_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_fx_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_gdp_per_capita') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_gdp_volume_growth') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_core_inflation') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_min_wage') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_adult_learning') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_extreme_poverty') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_industrial_production') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_construction_output') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_employment_enterprise') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_median_wage') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_price_m2_secondary') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_arope_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_long_term_unemployment') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_registered_unemployed') }}
)

select
    m.concept_key,
    m.source_system,
    m.measure_key,
    m.geography_key,
    m.date_key,
    m.value,
    b.value_min,
    b.value_max
from metrics as m
left join bounds as b
    on m.concept_key = b.concept_key and m.source_system = b.source_system
where m.value < b.value_min or m.value > b.value_max or b.value_min is null
