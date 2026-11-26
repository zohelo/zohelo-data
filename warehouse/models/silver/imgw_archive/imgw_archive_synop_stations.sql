{{ config(meta={'optional_parents': ['imgw_api_stations']}) }}
{# IMGW synoptic stations of the daily archive files (silver.imgw_archive_synop_daily), the station dimension of the
   synop observations. Grain: station_code (IMGW 9-digit code). Name = the name on the station's newest observation
   day (IMGW renamed some stations, e.g. Warszawa-Okęcie, Opis.txt errata), first and last observation day, days
   held. Coordinates and altitude only where the IMGW API station list (silver.imgw_api_stations) has the same code;
   no voivodeship or TERYT code, which IMGW does not give for these stations (Gold places a station in a voivodeship
   from its coordinates). api_synop_station_id links the archive to the API synop endpoint (id_stacji): for a
   synoptic station (code starting with 3) it is '12' and the last three digits of the code, the station number of
   the archive's own station list (wykaz_stacji.csv, Dev probe 43336937240: ŁEBA 354170120 → 120, WMO index 12120);
   null for other codes. #}
with days as (
    select
        station_code,
        arg_max(station_name, observation_date) as station_name,
        min(observation_date) as first_date,
        max(observation_date) as last_date,
        count(distinct observation_date) as days_held,
        arg_max(raw_sha256, observation_date) as raw_sha256
    from {{ source('imgw_archive_entities', 'imgw_archive_synop_daily') }}
    group by station_code
)

select
    d.station_code,
    d.station_name,
    d.first_date,
    d.last_date,
    d.days_held,
    s.longitude,
    s.latitude,
    s.altitude_m,
    s.station_code is not null as in_api_station_list,
    case when d.station_code like '3________' then '12' || right(d.station_code, 3) end as api_synop_station_id,
    d.raw_sha256
from days as d
left join {{ poc_optional_ref(
        'imgw_api_stations', 'station_code:varchar, longitude:double, latitude:double, altitude_m:double'
    ) }} as s
    on d.station_code = s.station_code
