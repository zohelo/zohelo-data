-- GIOŚ air quality stations (silver.gios_air_stations): the station list covers all 16 voivodeships and at least
-- 250 stations (288 on 12 Nov 2026). A truncated or re-shaped station list fails here.
select
    count(*) as stations,
    count(distinct voivodeship) as voivodeships
from {{ ref('gios_air_stations') }}
having count(*) < 250 or count(distinct voivodeship) <> 16
