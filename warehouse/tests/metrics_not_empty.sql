-- Gold metrics: every published KPI's metrics table has rows; an empty one would publish nothing.
with counts as (
    select
        'population' as concept_key,
        count(*) as metric_rows
    from {{ ref('poc_metrics_population') }}
    union all
    select
        'gdp_current_prices' as concept_key,
        count(*) as metric_rows
    from {{ ref('poc_metrics_gdp_current_prices') }}
    union all
    select
        'inflation' as concept_key,
        count(*) as metric_rows
    from {{ ref('poc_metrics_inflation') }}
    union all
    select
        'unemployment_rate' as concept_key,
        count(*) as metric_rows
    from {{ ref('poc_metrics_unemployment_rate') }}
    union all
    select
        'fx_rate' as concept_key,
        count(*) as metric_rows
    from {{ ref('poc_metrics_fx_rate') }}
)

select
    concept_key,
    metric_rows
from counts
where metric_rows = 0
