-- Our World in Data (CD-031): every chart of poc_owid_charts (the Gold blocks) has values in Silver, and no year
-- lies in or after the current one (the charts publish observations, not projections; the Global Carbon Budget
-- projects its release year). Silver keeps only the declared charts and value columns (owid_observations joins
-- poc_owid_charts), so undeclared columns are not checked here. Returns one row per problem.
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
    c.chart,
    case
        when l.chart is null then 'declared chart without values'
        else 'year in or after the current one: ' || l.last_year
    end as problem
from charts as c
left join landed as l on c.chart = l.chart and c.indicator = l.indicator
where l.chart is null or l.last_year >= extract(year from current_date)
