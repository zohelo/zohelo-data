{{ config(alias='build_imgw_archive', tags=['gold_block'], pre_hook="install spatial; load spatial") }}
{# Gold build stage: IMGW observation archive (register PL-ENV-009), Poland and voivodeship aggregates of the daily
   synoptic station data (silver.imgw_archive_synop_daily, IMGW-verified). Grain: measure_key x geography_key x
   date_key. Stations: every synoptic station of the archive files (57-63 a year since 1966) counts for Poland; a
   station counts for a voivodeship when its coordinates (silver.imgw_archive_synop_stations, from the IMGW API
   station list under the same 9-digit code) lie inside that voivodeship's PRG boundary (silver.prg_units). A
   station without coordinates (closed before the API list) counts for Poland only. Statistics:
   - block synop_daily (D): air temperature = mean of the station daily means (STD) of the stations of the area
     with a measured STD that day;
   - block synop_monthly (M): air temperature = mean over the area's stations of each station's monthly mean of
     its daily STD, for stations with a measured STD on every day of the month; precipitation = mean over the
     area's stations of each station's monthly sum of daily precipitation (SMDB), for stations with every day of
     the month either measured or "phenomenon absent" (status 9: no precipitation, 0 mm); a day with "no
     measurement" (status 8) leaves the station out of that month.
   Poland needs at least 30 stations for a day or month, a voivodeship at least one. One frequency per
   measure_key. source_version is the newest file version used; raw_sha256 is the SHA-256 of the sorted list of
   the Landing files used (an aggregate has no single file). A station-day-parameter held twice, or a station
   placed in two voivodeships, fails the build (no silent pick). #}
{% set min_stations_poland = 30 %}
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
        select 1 as held_twice
        from obs
        group by station_code, observation_date, parameter
        having count(*) > 1
    ) as twice
),

checked as (
    select o.*
    from obs as o
    cross join repeated as r
    where
        case
            when r.n > 0 then error('build_imgw_archive: ' || r.n || ' station-day-parameters held twice')
            else true
        end
),

voivodeships as (
    select
        teryt_code,
        st_geomfromwkb(geometry_wkb) as geom
    from {{ ref('prg_units') }}
    where unit_level = 'wojewodztwo'
),

placed as (
    select
        s.station_code,
        v.teryt_code,
        count(*) over (partition by s.station_code) as voivodeships_found
    from {{ ref('imgw_archive_synop_stations') }} as s
    inner join voivodeships as v on st_contains(v.geom, st_point(s.longitude, s.latitude))
    where s.longitude is not null and s.latitude is not null
),

areas as (
    select
        station_code,
        'PL' as geography_key,
        cast(null as varchar) as geography_source_code,
        {{ min_stations_poland }} as min_stations
    from {{ ref('imgw_archive_synop_stations') }}
    union all
    select
        station_code,
        'TERC:' || teryt_code as geography_key,
        teryt_code as geography_source_code,
        case
            when voivodeships_found > 1
                then error('build_imgw_archive: station ' || station_code || ' in two voivodeships')
            else 1
        end as min_stations
    from placed
),

daily as (
    select
        a.geography_key,
        a.geography_source_code,
        'synop_daily' as block_id,
        'STD' as parameter,
        'D' as frequency,
        o.observation_date as date_key,
        cast(o.observation_date as varchar) as period_label,
        avg(o.value) as value,
        max(o.source_version) as source_version,
        sha256(string_agg(distinct o.raw_sha256, ',' order by o.raw_sha256)) as raw_sha256
    from checked as o
    inner join areas as a on o.station_code = a.station_code
    where o.parameter = 'STD' and o.observation_status = 'ok'
    group by a.geography_key, a.geography_source_code, o.observation_date
    having count(*) >= max(a.min_stations)
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
    group by station_code, parameter, cast(date_trunc('month', observation_date) as date)
),

complete_month as (
    select
        station_code,
        parameter,
        month_start,
        case parameter when 'STD' then mean_value else coalesce(sum_measured, 0) end as value,
        source_version,
        files
    from station_month
    where
        (parameter = 'STD' and days_measured = day(last_day(month_start)))
        or (parameter = 'SMDB' and days_measured + days_absent = day(last_day(month_start)))
),

monthly as (
    select
        a.geography_key,
        a.geography_source_code,
        'synop_monthly' as block_id,
        m.parameter,
        'M' as frequency,
        m.month_start as date_key,
        strftime(m.month_start, '%Y-%m') as period_label,
        avg(m.value) as value,
        max(m.source_version) as source_version,
        sha256(string_agg(m.files, ',' order by m.files)) as raw_sha256
    from complete_month as m
    inner join areas as a on m.station_code = a.station_code
    group by a.geography_key, a.geography_source_code, m.parameter, m.month_start
    having count(*) >= max(a.min_stations)
),

observations as (
    select * from daily
    union all
    select * from monthly
)

select
    'imgw_archive' as source_system,
    block_id,
    case block_id
        when 'synop_daily' then 'IMGW synoptic stations, daily (Poland, voivodeships)'
        else 'IMGW synoptic stations, monthly (Poland, voivodeships)'
    end as block_name,
    'imgw_archive:' || block_id || ':' || case parameter when 'STD' then 'degC' else 'mm' end || ':' || parameter
        as measure_key,
    case
        when parameter = 'STD' and frequency = 'D'
            then 'Air temperature, daily mean, mean of IMGW synoptic stations (°C)'
        when parameter = 'STD' then 'Air temperature, monthly mean, mean of IMGW synoptic stations (°C)'
        else 'Precipitation, monthly total, mean of IMGW synoptic stations (mm)'
    end as measure_name,
    parameter as measure_codes,
    case parameter when 'STD' then 'degC' else 'mm' end as unit_code,
    case parameter when 'STD' then 'degrees Celsius' else 'millimetres' end as unit_name,
    geography_source_code,
    geography_key,
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
from observations
