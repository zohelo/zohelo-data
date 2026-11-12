-- Silver: GIOŚ air quality monitoring stations (register PL-ENV-010), from the station lists of the daily API passes.
-- Grain: station_id; the newest pass that listed the station wins (newest version), so a station that leaves the
-- list keeps its last attributes (last_listed_in = that pass). Source: bronze.gios_air_stations (station/findAll).
-- The API names the commune, district and voivodeship (upper case) but gives no TERYT codes; WGS84 as published.
select
    cast(station_id as bigint) as station_id,
    station_code,
    station_name,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(city_id as bigint) as city_id,
    city_name,
    commune,
    district,
    voivodeship,
    street,
    entity as last_listed_in,
    raw_sha256,
    source_version,
    ingested_at
from {{ source('gios_air_quality', 'gios_air_stations') }}
qualify ingested_at = max(ingested_at) over (partition by station_id)
