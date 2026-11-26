-- Silver: GIOŚ verified measurements from the yearly archive (register PL-ENV-010, #3119), one year file per entity.
-- Grain: station_code × indicator × averaging_time × measured_at (station code as the year's file prints it; codes
-- changed over the years, silver.gios_air_archive_stations lists the old codes of each station). Applied by
-- ingestion/to_bronze/gios_air_quality/archive_load.py (silver.gios_air_archive_values) to the year's one Bronze file.
-- Header cells are kept as published (indicator_published, averaging_time_published). indicator = the column's
-- Wskaźnik cell, or, where the file gives none (a workbook without that row, an empty cell), the indicator in the
-- member name '<year>_<indicator>_<averaging>.xlsx'. averaging_time is read from the member's period cells, not
-- from the header (which also holds texts such as 'SO2-24g' or '(PM2.5)-24g'): '1g' when the member's periods hold
-- hours, '24g' when they are whole days. measured_at = the period cell (Excel serial day number, 1899-12-30 = 0)
-- rounded to the minute; hourly values are stamped at the end of their hour (01:00 to 24:00), so measured_on, the
-- day a value describes, is the date one hour before the stamp; daily values are stamped at midnight of their day.
-- A cell that is not a number keeps value null and value_text (the loader rejects such a year, reject_if), so it
-- never becomes 0. Values are verified by GIOŚ: validation_status 'verified', never mixed with the provisional API
-- values of silver.gios_air_hourly.
with typed as (
    select
        b.*,
        timestamp '1899-12-30 00:00:00'
        + to_minutes(cast(round(cast(b.period as double) * 1440) as bigint)) as stamp,
        try_cast(b.value as double) as number,
        bool_or(cast(b.period as double) <> floor(cast(b.period as double))) over (partition by b.member) as hourly
    from bronze as b
)

select
    t.entity,
    cast(substr(t.entity, 9, 4) as integer) as archive_year,
    t.member,
    t.station_code,
    t.sensor_code,
    coalesce(t.indicator, nullif(regexp_extract(t.member, '^\d{4}_(.+)_(1g|24g)\.xlsx$', 1), '')) as indicator,
    case when t.hourly then '1g' else '24g' end as averaging_time,
    t.indicator as indicator_published,
    t.averaging_time as averaging_time_published,
    t.unit,
    t.stamp as measured_at,
    case
        when t.hourly then cast(t.stamp - interval 1 hour as date)
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
