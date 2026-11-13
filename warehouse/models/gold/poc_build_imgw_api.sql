{{ config(alias='build_imgw_api', tags=['gold_block']) }}
{# Gold build stage: IMGW public data API (register PL-ENV-008), Poland daily air temperature from the provisional
   synoptic snapshots (silver.imgw_api_synop). Its own source series (O31), never merged with the verified archive
   (imgw_archive); every row has observation status 'provisional'. Rule: a station counts for a UTC day when the
   snapshots hold its temperature at all eight main synoptic terms (00, 03, ..., 21 UTC); the station's day value
   is the mean of those eight; Poland = mean over the stations that count, published for a day with at least 30 of
   the 62 stations. A day whose snapshots missed a term (a scheduled run that did not happen) is not published.
   IMGW gives no voivodeship for the stations, so Poland only. #}
with terms as (
    select
        station_id,
        cast(observed_at as date) as observation_date,
        hour(observed_at) as term,
        temperature_c,
        raw_sha256,
        source_version
    from {{ ref('imgw_api_synop') }}
    where hour(observed_at) % 3 = 0 and temperature_c is not null
),

station_day as (
    select
        station_id,
        observation_date,
        avg(temperature_c) as value,
        max(source_version) as source_version,
        string_agg(distinct raw_sha256, ',') as files
    from terms
    group by station_id, observation_date
    having count(distinct term) = 8
),

poland as (
    select
        observation_date,
        avg(value) as value,
        max(source_version) as source_version,
        sha256(string_agg(files, ',' order by files)) as raw_sha256
    from station_day
    group by observation_date
    having count(*) >= 30
)

select
    'imgw_api' as source_system,
    'synop_daily' as block_id,
    'IMGW synoptic stations, provisional API snapshots, daily (Poland)' as block_name,
    'imgw_api:synop_daily:degC:temperatura_8_terms' as measure_key,
    'Air temperature, daily mean of the 8 main synoptic terms, mean of IMGW synoptic stations, provisional (°C)'
        as measure_name,
    'temperatura_8_terms' as measure_codes,
    'degC' as unit_code,
    'degrees Celsius' as unit_name,
    cast(null as varchar) as geography_source_code,
    'PL' as geography_key,
    'D' as frequency,
    observation_date as date_key,
    cast(observation_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    round(value, 2) as value,
    'provisional' as observation_status_key,
    source_version,
    raw_sha256
from poland
