{{ config(alias='gios_air_verified') }}
{# Gold: aggregates of the VERIFIED hourly values of GIOŚ's yearly archive (silver.gios_air_archive_values, #3119),
   the verified blocks of the build stage poc_build_gios_air_quality (read there through poc_optional_ref, so the
   provisional block never waits for the archive's first load). Kept apart from the provisional API values: own
   blocks and measure_keys, never the same series.
   Input: the automatic hourly ('1g') values of PM10, PM2.5, NO2, O3 and SO2; µg/m³ (the files from 2016 state
   'ug/m3' in their unit row, older files state no unit; any other unit fails the build). Manual daily ('24g')
   values are not used, as in the provisional block.
   Rule: a station-day counts with at least 18 of its 24 hourly values (75 % data capture, the provisional block's
   rule); its value is the mean of those hours; day = the day the hour belongs to (hours stamped 01:00 to 24:00).
   - block air_verified_daily (D), Poland only: mean of the station-day means of the day, with at least
     MIN_STATIONS_POLAND stations (a daily voivodeship series would rest on one to a dozen stations and add about
     500,000 rows; the voivodeships get the monthly series);
   - block air_verified_monthly (M), Poland and voivodeships: a station-month counts when at least 75 % of the
     month's days are counted station-days, its value is the mean of those days; the area value is the mean of its
     station-months, Poland with at least MIN_STATIONS_POLAND stations, a voivodeship with at least one.
   Voivodeship = the station's Województwo in the archive's station workbook (silver.gios_air_archive_stations),
   found by the station code of the year file: the current code, or one of the old codes the workbook lists for
   the station. An old code the workbook gives to several stations (a station moved or replaced; at least 10
   codes in Nov 2026) places its values only when those stations lie in one voivodeship, otherwise the build fails
   (no pick); a code found nowhere (mobile stations missing from the workbook) counts for Poland only.
   source_version = newest year file used; raw_sha256 = SHA-256 of the sorted list of the year files used (a station-day lies in one year file; one in two fails the build, and so does an hour that two workbooks of a
   year publish for one station and pollutant: Silver keeps both, here nothing is picked). #}
{% set min_stations_poland = 10 %}
{% set pollutants = "('PM10', 'PM2.5', 'NO2', 'O3', 'SO2')" %}
with hourly as (
    select
        station_code,
        indicator,
        measured_at,
        measured_on,
        value,
        unit,
        raw_sha256,
        source_version
    from {{ source('gios_air_quality_silver', 'gios_air_archive_values') }}
    where averaging_time = '1g' and indicator in {{ pollutants }}
),

checked as (
    select
        *,
        case
            when unit is not null and unit <> 'ug/m3'
                then error('gios_air_verified: unit ' || unit || ' for ' || indicator || ' at ' || station_code)
            else true
        end as unit_ok
    from hourly
),

codes as (
    select
        station_code as code,
        station_code,
        voivodeship
    from {{ ref('gios_air_archive_stations') }}
    union
    select
        trim(o.code) as code,
        o.station_code,
        o.voivodeship
    from (
        select
            station_code,
            voivodeship,
            unnest(string_split(regexp_replace(old_station_codes, '[;,\s]+', ',', 'g'), ',')) as code
        from {{ ref('gios_air_archive_stations') }}
        where old_station_codes is not null
    ) as o
    where trim(o.code) <> ''
),

placed as (
    select
        code,
        case
            when count(distinct voivodeship) > 1
                then error('gios_air_verified: station code ' || code || ' names stations of two voivodeships')
            else min(voivodeship)
        end as voivodeship
    from codes
    group by code
),

station_days as (
    select
        station_code,
        indicator,
        measured_on,
        avg(value) as day_mean,
        max(source_version) as source_version,
        case
            when count(*) <> count(distinct measured_at)
                then error(
                    'gios_air_verified: ' || station_code || ' ' || indicator || ' ' || measured_on
                    || ' has an hour in two workbooks'
                )
            when count(distinct raw_sha256) > 1
                then error(
                    'gios_air_verified: '
                    || station_code
                    || ' '
                    || indicator
                    || ' '
                    || measured_on
                    || ' in two year files'
                )
            else min(raw_sha256)
        end as raw_sha256
    from checked
    where unit_ok
    group by station_code, indicator, measured_on
    having count(*) >= 18
),

station_areas as (
    select distinct
        d.station_code,
        'POLSKA' as area,
        {{ min_stations_poland }} as min_stations
    from station_days as d
    union all
    select distinct
        d.station_code,
        p.voivodeship as area,
        1 as min_stations
    from station_days as d
    inner join placed as p on d.station_code = p.code
),

daily as (
    select
        'air_verified_daily' as block_id,
        'D' as frequency,
        indicator,
        'POLSKA' as area,
        measured_on as date_key,
        avg(day_mean) as value,
        count(*) as stations,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as raw_sha256
    from station_days
    group by indicator, measured_on
    having count(*) >= {{ min_stations_poland }}
),

station_months as (
    select
        station_code,
        indicator,
        cast(date_trunc('month', measured_on) as date) as month_start,
        avg(day_mean) as month_mean,
        max(source_version) as source_version,
        string_agg(distinct raw_sha256, ',' order by raw_sha256) as files
    from station_days
    group by station_code, indicator, cast(date_trunc('month', measured_on) as date)
    having count(*) >= 0.75 * day(last_day(cast(date_trunc('month', measured_on) as date)))
),

monthly as (
    select
        'air_verified_monthly' as block_id,
        'M' as frequency,
        m.indicator,
        a.area,
        m.month_start as date_key,
        avg(m.month_mean) as value,
        count(*) as stations,
        max(m.source_version) as source_version,
        sha256(string_agg(distinct m.files, ',' order by m.files)) as raw_sha256
    from station_months as m
    inner join station_areas as a on m.station_code = a.station_code
    group by m.indicator, a.area, m.month_start
    having count(*) >= max(a.min_stations)
)

select * from daily
union all
select * from monthly
