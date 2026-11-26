-- Silver: GIOŚ verified measurements from the yearly archive (register PL-ENV-010, #3119), one year file per entity.
-- Grain: station_code × indicator × averaging_time × measured_at × member (station code as the year's file prints
-- it; codes changed over the years, silver.gios_air_archive_stations lists the old codes of each station; the member
-- is part of the grain because two workbooks of a year can publish the same series, e.g. benzene at KpZielBoryTu in
-- both 2025_C6H6_1g.xlsx and 2025_PrekursoryZielonka_1g.xlsx: both rows are kept, none is picked). Applied by
-- ingestion/to_bronze/gios_air_quality/archive_load.py (silver.gios_air_archive_values) to the year's one Bronze file.
-- Header cells are kept as published (indicator_published, averaging_time_published). indicator = the column's
-- Wskaźnik cell, or, where the file gives none (a workbook without that row, an empty cell), the indicator in the
-- member name '<year>_<indicator>_<averaging>.xlsx'. averaging_time = the column's Czas uśredniania cell when it is a
-- code ('1g', '24g', '1t': 71 benzene cells of PdBialyMiejska 2013), else the averaging in the member name (the cell
-- is empty or holds other text: 'SO2-24g', '(PM2.5)-24g', station codes in 2010), else the member's period grid
-- ('1g' when its periods hold hours). It must agree with the grid: '1g' on a workbook of whole days, or another code
-- on a workbook of hours, leaves averaging_time null and the loader rejects the year (not_null), because the day a
-- value describes depends on it. measured_at = the period cell (Excel serial day number, 1899-12-30 = 0) rounded to
-- the hour (the serials drift by up to a minute within a year; Bronze rejects a period more than five minutes off a
-- whole hour); hourly values are stamped at the end of their hour (01:00 to 24:00), so measured_on, the day a value
-- describes, is the date one hour before the stamp; values of longer averaging times are stamped at midnight of their
-- day (a longer sample is written on each of its days, as published). Values with a decimal comma (text cells, 2016
-- to 2018) are read with a decimal point. A cell that is not a number keeps value null and value_text (the loader
-- rejects such a year, reject_if), so it never becomes 0. Values are verified by GIOŚ: validation_status 'verified',
-- never mixed with the provisional API values of silver.gios_air_hourly.
with typed as (
    select
        b.*,
        timestamp '1899-12-30 00:00:00' + to_hours(cast(round(cast(b.period as double) * 24) as bigint)) as stamp,
        try_cast(replace(b.value, ',', '.') as double) as number,
        nullif(regexp_extract(b.member, '[_-](1g|24g)\.xlsx$', 1), '') as member_averaging,
        bool_or(abs(cast(b.period as double) - round(cast(b.period as double))) > 1.0 / 48) over (partition by b.member)
        as hourly
    from bronze as b
),

averaged as (
    select
        t.*,
        case
            when t.averaging_time in ('1g', '24g', '1t') then t.averaging_time
            when t.member_averaging is not null then t.member_averaging
            when t.hourly then '1g'
            else '24g'
        end as averaging_code
    from typed as t
)

select
    a.entity,
    cast(substr(a.entity, 9, 4) as integer) as archive_year,
    a.member,
    a.station_code,
    a.sensor_code,
    coalesce(a.indicator, nullif(regexp_extract(a.member, '^\d{4}_(.+)_(1g|24g)\.xlsx$', 1), '')) as indicator,
    case when (a.averaging_code = '1g') = a.hourly then a.averaging_code end as averaging_time,
    a.indicator as indicator_published,
    a.averaging_time as averaging_time_published,
    a.unit,
    a.stamp as measured_at,
    case
        when a.averaging_code = '1g' then cast(a.stamp - interval 1 hour as date)
        else cast(a.stamp as date)
    end as measured_on,
    a.number as value,
    a.value as value_text,
    'verified' as validation_status,
    a.raw_sha256,
    a.source_version,
    a.ingested_at,
    a.sheet_row
from averaged as a
