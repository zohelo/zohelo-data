-- GIOŚ air quality sensors (silver.gios_air_sensors): at least 800 sensors, and each of the five pollutants of the
-- Gold block (PM10, PM2.5, NO2, O3, SO2) on at least 20 stations. A truncated sensor list fails here.
with per_code as (
    select
        p.code,
        count(distinct s.station_id) as stations
    from (values ('PM10'), ('PM2.5'), ('NO2'), ('O3'), ('SO2')) as p (code)
    left join {{ ref('gios_air_sensors') }} as s on p.code = s.parameter_code
    group by p.code
)

select
    (select count(*) from {{ ref('gios_air_sensors') }}) as sensors,
    min(stations) as fewest_stations
from per_code
having (select count(*) from {{ ref('gios_air_sensors') }}) < 800 or min(stations) < 20
