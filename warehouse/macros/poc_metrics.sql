{# Gold PoC metrics view of one concept: every measure of the concept at its native grain,
   total series only, one row per source × area × period, with the geography and calendar
   attributes a chart needs. Materialised as a table (the catalog holds no views). #}
{% macro poc_concept_metrics(concept_key) %}
with members as (
    select cm.measure_key, cm.definition, cm.source_system
    from {{ ref('poc_concept_measures') }} as cm
    where cm.concept_key = '{{ concept_key }}'
),
facts as (
    select * from {{ ref('poc_fact_annual') }}
    union all select * from {{ ref('poc_fact_monthly') }}
    union all select * from {{ ref('poc_fact_daily') }}
)

select
    '{{ concept_key }}' as concept_key,
    f.source_system,
    f.measure_key,
    m.measure_name,
    mb.definition,
    f.series_key,
    s.breakdown_json,
    f.geography_key,
    g.name as geography_name,
    g.level_name as geography_level,
    g.country_key,
    g.wojewodztwo_key,
    f.date_key,
    c.year,
    c.month_label,
    f.frequency,
    f.value,
    m.unit_key as unit,
    f.observation_status_key
from facts as f
inner join members as mb on mb.measure_key = f.measure_key
inner join {{ ref('poc_dim_series') }} as s on s.series_key = f.series_key
inner join {{ ref('poc_dim_measure') }} as m on m.measure_key = f.measure_key
left join {{ ref('poc_dim_geography') }} as g on g.geography_key = f.geography_key
left join {{ ref('poc_dim_calendar') }} as c on c.date_key = f.date_key
where s.is_total
{% endmacro %}
