-- Our World in Data (CD-031): the metadata.json of every chart of poc_owid_charts describes its value column
-- with the unit the Gold measure declares (published_unit) and a lastUpdated date, so a chart whose unit or
-- version field changes upstream fails here instead of reaching Gold under the old unit. Returns one row per
-- problem.
with charts as ({{ poc_owid_charts() }}),

meta as (
    select
        chart,
        short_name,
        coalesce(unit, '') as unit,
        last_updated
    from {{ ref('owid_charts') }}
)

select
    c.chart,
    case
        when m.chart is null then 'no metadata for the value column ' || c.indicator
        when m.unit <> c.published_unit then 'unit "' || m.unit || '", declared "' || c.published_unit || '"'
        else 'lastUpdated missing or not a date'
    end as problem
from charts as c
left join meta as m on c.chart = m.chart and c.indicator = m.short_name
where m.chart is null or m.unit <> c.published_unit or m.last_updated is null
