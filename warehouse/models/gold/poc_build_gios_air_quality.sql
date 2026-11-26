{{ config(alias='build_gios_air_quality', tags=['gold_block'], meta={'optional_parents': ['poc_gios_air_verified']}) }}
{# Gold build stage: GIOŚ air quality (register PL-ENV-010), daily means of the hourly API values in
   silver.gios_air_hourly (provisional: GIOŚ does not verify them before publishing). Block 'air_daily', frequency D,
   one measure_key per pollutant (PM10, PM2.5, NO2, O3, SO2; µg/m³ as published, D33).
   Rule: a sensor-day counts when the sensor has at least 18 of 24 hourly values that day (75 % data capture; a null
   value is missing, never 0); its value is the mean of those hours. The voivodeship value is the mean of the
   sensor-day values of the stations in the voivodeship (the station's own Województwo field), Poland the mean over
   all stations; a station with two sensors of one pollutant counts each sensor. Day = local date of the value as
   published. Status 'provisional'. The ozone value is a plain daily mean, not the 8-hour maximum of the EU limit.
   Blocks 'air_verified_daily' (D, Poland) and 'air_verified_monthly' (M, Poland and voivodeships): the verified
   values of the yearly archive (#3119), aggregated by models/gold/poc_gios_air_verified.sql and read here through
   poc_optional_ref (zero rows until silver.gios_air_archive_values exists, so the provisional block never waits);
   their own measure_keys ('air_verified_*'), status 'ok' (GIOŚ verified them), never one series with the
   provisional values. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gios_air_quality'
),

sensors as (
    select
        s.sensor_id,
        s.parameter_code,
        st.voivodeship
    from {{ ref('gios_air_sensors') }} as s
    inner join {{ ref('gios_air_stations') }} as st on s.station_id = st.station_id
    where s.parameter_code in ('PM10', 'PM2.5', 'NO2', 'O3', 'SO2')
),

sensor_days as (
    select
        h.sensor_id,
        h.measured_on,
        avg(h.value) as day_mean,
        max(h.source_version) as source_version,
        arg_max(h.raw_sha256, h.ingested_at) as raw_sha256
    from {{ source('gios_air_quality_silver', 'gios_air_hourly') }} as h
    where h.sensor_id in (select s.sensor_id from sensors as s)
    group by h.sensor_id, h.measured_on
    having count(h.value) >= 18
),

located as (
    select
        s.parameter_code,
        s.voivodeship,
        d.measured_on,
        d.day_mean,
        d.source_version,
        d.raw_sha256
    from sensor_days as d
    inner join sensors as s on d.sensor_id = s.sensor_id
),

areas as (
    select
        l.parameter_code,
        l.voivodeship as area,
        l.measured_on,
        avg(l.day_mean) as value,
        max(l.source_version) as source_version,
        arg_max(l.raw_sha256, l.source_version) as raw_sha256
    from located as l
    group by l.parameter_code, l.voivodeship, l.measured_on
    union all
    select
        l.parameter_code,
        'POLSKA' as area,
        l.measured_on,
        avg(l.day_mean) as value,
        max(l.source_version) as source_version,
        arg_max(l.raw_sha256, l.source_version) as raw_sha256
    from located as l
    group by l.parameter_code, l.measured_on
),

provisional as (
    select
        'gios_air_quality' as source_system,
        'air_daily' as block_id,
        'GIOŚ air quality, daily means of hourly values (provisional)' as block_name,
        'gios_air_quality:air_daily:ug_m3:' || a.parameter_code as measure_key,
        'Daily mean concentration of '
        || a.parameter_code
        || ' (GIOŚ stations, at least 18 hourly values)' as measure_name,
        a.parameter_code as measure_codes,
        'ug_m3' as unit_code,
        'μg/m3' as unit_name,
        a.area as geography_source_code,
        coalesce(g.geography_key, '_X') as geography_key,
        'D' as frequency,
        a.measured_on as date_key,
        cast(a.measured_on as varchar) as period_label,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        cast(null as varchar) as sex_code,
        true as is_total,
        a.value,
        'provisional' as observation_status_key,
        a.source_version,
        a.raw_sha256
    from areas as a
    left join geo as g on a.area = g.source_code
),

verified as (
    select
        v.block_id,
        v.frequency,
        v.indicator,
        v.area,
        v.date_key,
        v.value,
        v.source_version,
        v.raw_sha256
    from {{ poc_optional_ref('poc_gios_air_verified', 'block_id:varchar, frequency:varchar, indicator:varchar, '
        ~ 'area:varchar, date_key:date, value:double, stations:bigint, source_version:varchar, raw_sha256:varchar') }} as v
)

select * from provisional
union all
select
    'gios_air_quality' as source_system,
    v.block_id,
    case v.block_id
        when 'air_verified_daily' then 'GIOŚ air quality, daily means of verified hourly values (Poland)'
        else 'GIOŚ air quality, monthly means of verified hourly values (Poland, voivodeships)'
    end as block_name,
    'gios_air_quality:' || v.block_id || ':ug_m3:' || v.indicator as measure_key,
    case v.frequency
        when 'D'
            then
                'Daily mean concentration of ' || v.indicator
                || ', verified (GIOŚ archive, stations with at least 18 hourly values)'
        else
            'Monthly mean concentration of ' || v.indicator
            || ', verified (GIOŚ archive, station-months with at least 75 % of days)'
    end as measure_name,
    v.indicator as measure_codes,
    'ug_m3' as unit_code,
    'μg/m3' as unit_name,
    v.area as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    v.frequency,
    v.date_key,
    case v.frequency when 'D' then cast(v.date_key as varchar) else strftime(v.date_key, '%Y-%m') end as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    round(v.value, 2) as value,
    'ok' as observation_status_key,
    v.source_version,
    v.raw_sha256
from verified as v
left join geo as g on v.area = g.source_code
