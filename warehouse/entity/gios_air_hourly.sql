-- Silver: GIOŚ air quality hourly measurements (register PL-ENV-010), one measurement day per file.
-- Grain: sensor_id × measured_at (local time as published). Applied by ingestion/to_bronze/gios_air_quality/entities.py (silver.gios_air_hourly) to
-- the Bronze files of the daily API passes that cover the day (`bronze`), for the day in `target_day`. Each pass holds
-- the last three days, so an hour is in up to four passes: the newest pass that holds the hour wins (all its rows for
-- that key are kept, so a key repeated inside one pass fails the grain check instead of being picked). The API values
-- are unverified (GIOŚ), hence validation_status 'provisional'. A missing value stays null, never 0.
with typed as (
    select
        cast(station_id as bigint) as station_id,
        cast(sensor_id as bigint) as sensor_id,
        sensor_code,
        strptime(measured_at, '%Y-%m-%d %H:%M:%S') as measured_at,
        cast(value as double) as value,
        raw_sha256,
        source_version,
        ingested_at
    from bronze
)

select
    strftime(cast(t.measured_at as date), '%Y-%m-%d') as entity,
    cast(t.measured_at as date) as measured_on,
    t.measured_at,
    t.station_id,
    t.sensor_id,
    t.sensor_code,
    t.value,
    'provisional' as validation_status,
    t.raw_sha256,
    t.source_version,
    t.ingested_at
from typed as t
where cast(t.measured_at as date) = (select d.day from target_day as d)
qualify t.ingested_at = max(t.ingested_at) over (partition by t.sensor_id, t.measured_at)
