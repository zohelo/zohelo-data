-- wody_polskie_apgw: a plan cycle has at least 3,000 river, 500 lake, 5 transitional and 3 coastal water bodies (Dev
-- probe 45921328581: 3,161 RW incl. 45 reservoirs, 1,068 LW, 7 TW, 4 CW in the 2nd update); every lake, transitional,
-- coastal and reservoir body has its published area (a lost area layer or a shifted field fails here); every published
-- voivodeship list gives one TERYT voivodeship code per ';'-separated part, each a code of the 16 voivodeships. One
-- test per Silver model (it reads only this model).
with counts as (
    select
        plan_cycle,
        count(*) filter (where category = 'river') as rivers,
        count(*) filter (where category = 'lake') as lakes,
        count(*) filter (where category = 'transitional') as transitional,
        count(*) filter (where category = 'coastal') as coastal
    from {{ ref('wody_polskie_apgw_surface_water_bodies') }}
    group by plan_cycle
)

select
    'too few water bodies in ' || plan_cycle as problem,
    1 as found
from counts
where rivers < 3000 or lakes < 500 or transitional < 5 or coastal < 3
union all
select
    'no published area: ' || jcwp_code as problem,
    1 as found
from {{ ref('wody_polskie_apgw_surface_water_bodies') }}
where (category <> 'river' or is_reservoir) and (area_km2 is null or area_km2 <= 0)
union all
select
    'voivodeship list not parsed: ' || jcwp_code || ' ' || coalesce(voivodeships, 'NULL') as problem,
    1 as found
from {{ ref('wody_polskie_apgw_surface_water_bodies') }}
where
    voivodeships is null
    or len(string_split(voivodeship_codes, ',')) <> voivodeship_parts
    or not regexp_full_match(
        voivodeship_codes,
        '(02|04|06|08|10|12|14|16|18|20|22|24|26|28|30|32)(,(02|04|06|08|10|12|14|16|18|20|22|24|26|28|30|32))*'
    )
