{# IMGW automatic stations as the public data API lists them (/api/data/meteo/), newest landed snapshot
   (bronze.imgw_api_meteo). Grain: station_code (IMGW 9-digit code, the code of the archive files). Name,
   coordinates (WGS 84 longitude and latitude as IMGW gives them), altitude in m a.s.l. and founding year as
   published; the 10-minute readings of the endpoint are not modelled. No voivodeship or TERYT code: the
   endpoint gives none. #}
with newest as (
    select *
    from {{ source('imgw_api', 'imgw_api_meteo') }}
    qualify rank() over (order by ingested_at desc) = 1
)

select
    kod_stacji as station_code,
    nazwa_stacji as station_name,
    try_cast(lon as double) as longitude,
    try_cast(lat as double) as latitude,
    try_cast(wysokosc_npm as double) as altitude_m,
    try_cast(rok_zalozenia_stacji as integer) as founded_year,
    raw_sha256,
    source_version
from newest
