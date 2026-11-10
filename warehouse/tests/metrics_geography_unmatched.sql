-- Gold metrics: the total-series observations of a KPI's measures that the metrics tables leave out
-- because their area is unmatched (`_X`, #2307). A warning, not a failure: the build annotation lists
-- the count, and gold.build_check names the source codes (unmatched_sample) to map or accept.
{{ config(severity='warn') }}

with members as (
    select distinct
        concept_key,
        measure_key
    from {{ ref('poc_concept_measures') }}
    where concept_key in ('population', 'gdp_current_prices', 'inflation', 'unemployment_rate', 'fx_rate')
),

facts as (
    select series_key, measure_key, source_system, geography_key, date_key from {{ ref('poc_fact_annual') }}
    union all
    select series_key, measure_key, source_system, geography_key, date_key from {{ ref('poc_fact_quarterly') }}
    union all
    select series_key, measure_key, source_system, geography_key, date_key from {{ ref('poc_fact_monthly') }}
    union all
    select series_key, measure_key, source_system, geography_key, date_key from {{ ref('poc_fact_daily') }}
)

select
    m.concept_key,
    f.source_system,
    f.measure_key,
    f.series_key,
    f.date_key
from facts as f
inner join members as m on f.measure_key = m.measure_key
inner join {{ ref('poc_dim_series') }} as s on f.series_key = s.series_key
where s.is_total and f.geography_key = '_X'
