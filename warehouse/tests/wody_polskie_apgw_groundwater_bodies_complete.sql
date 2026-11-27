-- wody_polskie_apgw: a plan cycle has at least 150 groundwater bodies (Dev probe 45921328581: 174 in the 2nd update),
-- each with a published area above 0 and with its characterisation rows (a lost layer or a renamed code fails here),
-- and the district taken from the code is one of the districts the characterisation names for the body
-- (Charakterystyka_JCWPd prints the district's short name: 1000 Dunaj, 2000 Wisła, 3000 Świeża, 4000 Banówka,
-- 5000 Łaba, 6000 Odra, 7000 Pregoła, 8000 Niemen, 9000 Dniestr, the codes of the geodatabase's Obszary_dorzeczy
-- layer, paired through its water regions: 4000BA Banówka, 7000LW Łyna and Węgorapa, 8000NI Niemen, 9000DN Dniestr).
-- One test per Silver model (it reads only this model).
select
    'too few groundwater bodies in ' || plan_cycle as problem,
    count(*) as found
from {{ ref('wody_polskie_apgw_groundwater_bodies') }}
group by plan_cycle
having count(*) < 150
union all
select
    'no area or no characterisation: ' || jcwpd_code as problem,
    1 as found
from {{ ref('wody_polskie_apgw_groundwater_bodies') }}
where area_km2 is null or area_km2 <= 0 or characteristic_rows = 0
union all
select
    'district of the code not named in the characterisation: ' || jcwpd_code || ' ' || coalesce(
        river_basin_district_code, 'NULL'
    ) || ' / ' || coalesce(river_basin_districts, 'NULL') as problem,
    1 as found
from {{ ref('wody_polskie_apgw_groundwater_bodies') }}
where
    position(
        case river_basin_district_code
            when '1000' then 'Dunaj'
            when '2000' then 'Wisła'
            when '3000' then 'Świeża'
            when '4000' then 'Banówka'
            when '5000' then 'Łaba'
            when '6000' then 'Odra'
            when '7000' then 'Pregoła'
            when '8000' then 'Niemen'
            when '9000' then 'Dniestr'
        end in coalesce(river_basin_districts, '')
    ) = 0
    or river_basin_district_code is null
