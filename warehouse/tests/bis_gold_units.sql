-- BIS (#2861): gold.build_bis names the unit of every built series from macro poc_bis_measures; the BIS sends the
-- unit as UNIT_MEASURE on the series. Fails with the series whose UNIT_MEASURE in silver.bis_observations is no
-- longer the one the macro expects (a rebased index, a changed unit), so a unit never changes under a measure key.
with measures as ({{ poc_bis_measures() }}),

series as (
    select
        o.entity,
        o.series_key,
        o.unit_measure,
        {{ poc_bis_series_code() | trim }} as series_code
    from {{ ref('bis_observations') }} as o
    where o.obs_value is not null
    group by all
)

select
    s.entity,
    s.series_key,
    s.unit_measure,
    m.source_unit as expected_unit_measure
from series as s
inner join measures as m on s.entity = m.entity and s.series_code = m.series_code
where s.unit_measure is distinct from m.source_unit
