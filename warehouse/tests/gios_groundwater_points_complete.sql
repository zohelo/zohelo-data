-- gios_groundwater: every year has at least 300 monitoring points (Dev probe 45810873298: 348 to 1,374 points a
-- year in the inorganic files), each with both PUWG 1992 coordinates inside Poland's EPSG:2180 extent; a lost file,
-- a shifted column or swapped coordinates fails here. One test per Silver model (it reads only this model).
select
    'points in ' || survey_year as problem,
    count(*) as found
from {{ ref('gios_groundwater_points') }}
group by survey_year
having count(*) < 300
union all
select
    'coordinates outside Poland: ' || point_key || ' ' || survey_year as problem,
    1 as found
from {{ ref('gios_groundwater_points') }}
where
    coalesce(conflicting_attributes, '') not like '%puwg1992%'
    and (
        puwg1992_x is null or puwg1992_y is null
        or puwg1992_x not between 140000 and 900000
        or puwg1992_y not between 100000 and 800000
    )
