-- cckp_era5: geonames.json lists at least 240 countries and Poland's 16 first-level units, and each code of the stated
-- voivodeship map (poc_cckp_era5_voivodeships() in macros/sources/cckp_era5.sql, read by the Gold geography map) still
-- carries the name it had when the map was written (Dev probe 45810479823). A renumbered or renamed unit fails here,
-- so a voivodeship is never mapped to the wrong TERC code. One test per Silver model (it reads only this model).
with units as (select * from {{ ref('cckp_era5_units') }}),

stated as ({{ poc_cckp_era5_voivodeships() }})

select
    'countries' as problem,
    count(*) as found
from units
where unit_level = 'country'
having count(*) < 240
union all
select
    'first-level units of POL' as problem,
    count(*) as found
from units
where parent_geocode = 'POL'
having count(*) <> 16
union all
select
    'stated code missing or renamed: ' || s.geocode || ' ' || s.cckp_name as problem,
    1 as found
from stated as s
left join units as u on s.geocode = u.geocode and u.parent_geocode = 'POL'
where u.name is distinct from s.cckp_name
