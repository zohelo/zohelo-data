-- Silver: GIOŚ air quality sensors (measuring positions) of the stations (register PL-ENV-010), from the sensor lists
-- of the daily API passes. Grain: sensor_id; the newest pass that listed the sensor wins (newest version).
-- Source: bronze.gios_air_sensors (station/sensors/{id}); parameter code and name as published. The getData service
-- states µg/m³ for SO2, NO2, PM10, PM2.5, CO, C6H6 and O3 only; the unit of the other codes is not stated there.
select
    cast(sensor_id as bigint) as sensor_id,
    cast(station_id as bigint) as station_id,
    cast(parameter_id as bigint) as parameter_id,
    parameter_code,
    parameter_formula,
    parameter_name,
    entity as last_listed_in,
    raw_sha256,
    source_version,
    ingested_at
from {{ source('gios_air_quality', 'gios_air_sensors') }}
qualify ingested_at = max(ingested_at) over (partition by sensor_id)
