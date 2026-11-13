-- BIS (#2861): gold.build_bis joins silver.bis_observations to macro poc_bis_measures on the exact series code.
-- If the BIS renames a code (or a selection stops carrying a series), the measure would lose its rows without
-- any other test failing. Returns the listed series that match no row with a value in Silver.
with measures as ({{ poc_bis_measures() }}),

series as (
    select distinct
        o.entity,
        {{ poc_bis_series_code() | trim }} as series_code
    from {{ ref('bis_observations') }} as o
    where o.obs_value is not null
)

select
    m.entity,
    m.series_code,
    m.measure_name
from measures as m
left join series as s on m.entity = s.entity and m.series_code = s.series_code
where s.entity is null
