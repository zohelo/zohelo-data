-- Gold metrics: one row per KPI × source × measure × geography × frequency × period (fx_rate also
-- per currency, in breakdown_json). latest.json reads the two newest rows of one series per source ×
-- geography, so a second total series of one measure and area would make it pick a row at random.
with metrics as (
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        frequency,
        date_key,
        breakdown_json
    from {{ ref('poc_metrics_population') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        frequency,
        date_key,
        breakdown_json
    from {{ ref('poc_metrics_gdp_current_prices') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        frequency,
        date_key,
        breakdown_json
    from {{ ref('poc_metrics_inflation') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        frequency,
        date_key,
        breakdown_json
    from {{ ref('poc_metrics_unemployment_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        frequency,
        date_key,
        breakdown_json
    from {{ ref('poc_metrics_fx_rate') }}
)

select
    concept_key,
    source_system,
    measure_key,
    geography_key,
    frequency,
    date_key,
    case when concept_key = 'fx_rate' then breakdown_json end as breakdown,
    count(*) as rows_per_key
from metrics
group by all
having count(*) > 1
