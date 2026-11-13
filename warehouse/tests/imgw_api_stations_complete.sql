{{ config(severity='warn') }}
-- IMGW API station list (silver.imgw_api_stations): at least 500 stations with coordinates (788 on 13 Nov 2026, Dev
-- probe 43336937240). Fewer means the meteo snapshot was partial.
select count(*) as stations_with_coordinates
from {{ ref('imgw_api_stations') }}
where longitude is not null and latitude is not null
having count(*) < 500
