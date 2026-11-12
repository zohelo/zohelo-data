-- UNdata (#2861): gold.build_undata takes the series of macro poc_undata_series by their exact text as published.
-- A new Yearbook edition that renames a series would drop the measure without any other test failing. Returns the
-- listed series that match no row with a value in silver.undata_syb_values.
with series as ({{ poc_undata_series() }}),

present as (
    select
        entity,
        series,
        count(*) as silver_rows
    from {{ ref('undata_syb_values') }}
    where value is not null
    group by entity, series
)

select
    s.entity,
    s.series
from series as s
left join present as p on s.entity = p.entity and s.series = p.series
where p.silver_rows is null
