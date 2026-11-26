-- Silver: GIOŚ verified measurements from the yearly archive (register PL-ENV-010, #3119), one year file per entity.
-- Grain: station_code × indicator × averaging_time × measured_at (station code as the year's file prints it; codes
-- changed over the years, silver.gios_air_archive_stations lists the old codes of each station). Applied by
-- ingestion/to_bronze/gios_air_quality/archive_load.py (silver.gios_air_archive_values) to the year's one Bronze file.
-- measured_at = the period cell (Excel serial day number, 1899-12-30 = 0) rounded to the minute: hourly values ('1g')
-- are stamped at the end of their hour (01:00 to 24:00, so measured_on, the day the value describes, is the date
-- one hour before the stamp); values of longer averaging times are stamped at midnight of their day
-- (measured_on = that date). Values are numbers as published (µg/m³ or as the unit row says; years before the unit
-- row give none); a cell that is not a number keeps value null and value_text, and the loader rejects the year
-- (reject_if), so it never becomes 0. Values are verified by GIOŚ: validation_status 'verified', never mixed with
-- the provisional API values of silver.gios_air_hourly.
with typed as (
    select
        b.*,
        timestamp '1899-12-30 00:00:00'
        + to_minutes(cast(round(cast(b.period as double) * 1440) as bigint)) as stamp,
        try_cast(b.value as double) as number
    from bronze as b
)

select
    t.entity,
    cast(substr(t.entity, 9, 4) as integer) as archive_year,
    t.member,
    t.station_code,
    t.sensor_code,
    t.indicator,
    t.averaging_time,
    t.unit,
    t.stamp as measured_at,
    case
        when t.averaging_time = '1g' then cast(t.stamp - interval 1 hour as date)
        else cast(t.stamp as date)
    end as measured_on,
    t.number as value,
    t.value as value_text,
    'verified' as validation_status,
    t.raw_sha256,
    t.source_version,
    t.ingested_at,
    t.sheet_row
from typed as t
