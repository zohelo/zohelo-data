-- gus_strateg: the SOR list held 83 indicator dimensions (Dev probe 45746504682, 26 Nov 2026). A list cut short (fewer
-- than 80) or an indicator without the levels and years STRATEG gives for it fails here instead of thinning Gold
-- silently. One test per Silver model (it reads only this model).
select
    'indicator dimensions in the SOR list' as problem,
    count(*) as found
from {{ ref('gus_strateg_indicators') }}
having count(*) < 80
union all
select
    'no levels or years: ' || indicator_id || '.' || dimension_id as problem,
    1 as found
from {{ ref('gus_strateg_indicators') }}
where territory_levels_json is null or years_json is null
