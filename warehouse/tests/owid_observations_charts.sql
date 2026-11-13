-- Our World in Data (CD-031): every chart of poc_owid_charts (the Gold blocks) has values in Silver, the
-- values of each chart sit in its declared value column only, and no year lies after the current one (the charts
-- publish observations, not projections). Returns one row per problem.
with charts as ({{ poc_owid_charts() }}),

landed as (
    select
        chart,
        indicator,
        count(*) as observations,
        max(year) as last_year
    from {{ ref('owid_observations') }}
    group by chart, indicator
)

select
    coalesce(c.chart, l.chart) as chart,
    case
        when l.chart is null then 'declared chart without values'
        when c.chart is null then 'values in an undeclared chart or column'
        else 'year after the current one: ' || l.last_year
    end as problem
from charts as c
full outer join landed as l on c.chart = l.chart and c.indicator = l.indicator
where l.chart is null or c.chart is null or l.last_year > extract(year from current_date)
