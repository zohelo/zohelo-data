{{ config(alias='build_imgw_archive', tags=['gold_block']) }}
{# Gold build stage: IMGW observation archive (register PL-ENV-009), Poland aggregates of the daily synoptic station
   data (silver.imgw_archive_synop_daily, IMGW-verified). IMGW gives no voivodeship for its stations, so Poland only.
   Rule, over the synoptic stations of the archive files (57-63 a year since 1966):
   - block synop_daily (D): air temperature, Poland = mean of the station daily means (STD) of every station
     with a measured STD that day;
   - block synop_monthly (M): air temperature, Poland = mean over stations of the station's monthly mean of
     its daily STD, for stations with a measured STD on every day of the month; precipitation, Poland = mean over
     stations of the station's monthly sum of daily precipitation (SMDB), for stations with every day of the month
     either measured or "phenomenon absent" (status 9, no precipitation: 0 mm); a day with "no measurement"
     (status 8) leaves the station out of that month.
   A day or month counts only with at least 30 stations. One frequency per measure_key. source_version is the newest
   file version used; raw_sha256 is the SHA-256 of the sorted list of the Landing files used (an aggregate has no
   single file). A station-day-parameter held twice fails the build (no silent pick). #}
{% set min_stations = 30 %}
with obs as (
    select
        station_code,
        observation_date,
        parameter,
        value,
        observation_status,
        raw_sha256,
        source_version
    from {{ source('imgw_archive_entities', 'imgw_archive_synop_daily') }}
    where parameter in ('STD', 'SMDB')
),

repeated as (
    select count(*) as n
    from (
        select 1
        from obs
        group by station_code, observation_date, parameter
        having count(*) > 1
    )
),

checked as (
    select o.*
    from obs as o
    cross join repeated as r
    where case when r.n > 0 then error('build_imgw_archive: ' || r.n || ' station-day-parameters held twice') else true end
),

daily as (
    select
        'synop_daily' as block_id,
        'STD' as parameter,
        'D' as frequency,
        observation_date as date_key,
        cast(observation_date as varchar) as period_label,
        avg(value) as value,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as raw_sha256
    from checked
    where parameter = 'STD' and observation_status = 'ok'
    group by observation_date
    having count(*) >= {{ min_stations }}
),

station_month as (
    select
        station_code,
        parameter,
        cast(date_trunc('month', observation_date) as date) as month_start,
        count(*) filter (where observation_status = 'ok') as days_measured,
        count(*) filter (where observation_status = 'phenomenon_absent') as days_absent,
        avg(value) filter (where observation_status = 'ok') as mean_value,
        sum(value) filter (where observation_status = 'ok') as sum_measured,
        max(source_version) as source_version,
        string_agg(distinct raw_sha256, ',') as files
    from checked
    group by all
),

monthly as (
    select
        'synop_monthly' as block_id,
        parameter,
        'M' as frequency,
        month_start as date_key,
        strftime(month_start, '%Y-%m') as period_label,
        avg(case parameter when 'STD' then mean_value else coalesce(sum_measured, 0) end) as value,
        max(source_version) as source_version,
        sha256(string_agg(files, ',' order by files)) as raw_sha256
    from station_month
    where (parameter = 'STD' and days_measured = day(last_day(month_start)))
        or (parameter = 'SMDB' and days_measured + days_absent = day(last_day(month_start)))
    group by parameter, month_start
    having count(*) >= {{ min_stations }}
),

rows as (
    select * from daily
    union all
    select * from monthly
)

select
    'imgw_archive' as source_system,
    block_id,
    case block_id
        when 'synop_daily' then 'IMGW synoptic stations, daily (Poland)'
        else 'IMGW synoptic stations, monthly (Poland)'
    end as block_name,
    'imgw_archive:' || block_id || ':' || case parameter when 'STD' then 'degC' else 'mm' end || ':' || parameter as measure_key,
    case
        when parameter = 'STD' and frequency = 'D'
            then 'Air temperature, daily mean, mean of IMGW synoptic stations (°C)'
        when parameter = 'STD' then 'Air temperature, monthly mean, mean of IMGW synoptic stations (°C)'
        else 'Precipitation, monthly total, mean of IMGW synoptic stations (mm)'
    end as measure_name,
    parameter as measure_codes,
    case parameter when 'STD' then 'degC' else 'mm' end as unit_code,
    case parameter when 'STD' then 'degrees Celsius' else 'millimetres' end as unit_name,
    cast(null as varchar) as geography_source_code,
    'PL' as geography_key,
    frequency,
    date_key,
    period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    round(value, 2) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from rows
