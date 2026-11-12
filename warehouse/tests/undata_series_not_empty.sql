-- UNdata (#2861): gold.build_undata joins silver.undata_syb_values to macro poc_undata_series on the exact series
-- text of the UN Statistical Yearbook (the Yearbook has no series codes). A new edition that rewords a series
-- ("GDP real rates of growth (percent)") would empty the measure without any other test failing. Returns the
-- listed series that match no row with a value in Silver.
with listed as ({{ poc_undata_series() }}),

series as (
    select distinct
        o.entity,
        o.series
    from {{ ref('undata_syb_values') }} as o
    where o.value is not null
)

select
    l.entity,
    l.series,
    l.measure_code
from listed as l
left join series as s on l.entity = s.entity and l.series = s.series
where s.entity is null
