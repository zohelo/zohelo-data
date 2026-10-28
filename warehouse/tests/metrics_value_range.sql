-- Gold metrics: every value lies within a plausible range for its KPI and source. The bounds catch
-- unit and scale errors (a value ×1000, a percentage stored as a share), not small mistakes. Change
-- a bound only with the publisher's own value as evidence, cited in a comment next to it.
-- A KPI × source without a bound fails, so a new source gets one here.
with bounds (concept_key, source_system, value_min, value_max) as (
    values
    ('population', 'eurostat', 0, 1e9),
    ('population', 'world_bank_wdi', 0, 1e10),
    ('population', 'gus_bdl', 0, 5e7),
    ('population', 'gus_dbw', 0, 5e7),
    ('gdp_current_prices', 'eurostat', 0, 3e7),
    ('gdp_current_prices', 'world_bank_wdi', 0, 3e14),
    ('inflation', 'eurostat', -100, 3000),
    ('inflation', 'world_bank_wdi', -100, 100000),
    ('unemployment_rate', 'eurostat', 0, 60),
    ('unemployment_rate', 'world_bank_wdi', 0, 60),
    ('fx_rate', 'nbp', 0, 100000)
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
