-- wody_polskie_apgw: a plan cycle has the 9 river basin districts of Poland (RBD_MS_CD 1000 to 9000), at least 20 water
-- regions, each in one of those districts, and at least 10 RZGW areas (Dev probe 45921328581: 9, 23 and 11), each unit
-- with a published area above 0. One test per Silver model (it reads only this model).
with kinds as (
    select
        plan_cycle,
        count(*) filter (where unit_kind = 'river_basin_district') as districts,
        count(*) filter (where unit_kind = 'water_region') as regions,
        count(*) filter (where unit_kind = 'rzgw') as rzgws
    from {{ ref('wody_polskie_apgw_planning_units') }}
    group by plan_cycle
)

select
    'unit counts in ' || plan_cycle || ': ' || districts || '/' || regions || '/' || rzgws as problem,
    1 as found
from kinds
where districts <> 9 or regions < 20 or rzgws < 10
union all
select
    'unknown district or no area: ' || unit_kind || ' ' || unit_code as problem,
    1 as found
from {{ ref('wody_polskie_apgw_planning_units') }}
where
    area_km2 is null
    or area_km2 <= 0
    or (unit_kind = 'river_basin_district' and unit_code not in (
        '1000', '2000', '3000', '4000', '5000', '6000', '7000', '8000', '9000'
    ))
    or (unit_kind = 'water_region' and coalesce(river_basin_district_code, '') not in (
        '1000', '2000', '3000', '4000', '5000', '6000', '7000', '8000', '9000'
    ))
