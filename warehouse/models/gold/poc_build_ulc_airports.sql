{{ config(alias='build_ulc_airports', tags=['gold_block']) }}
{# Gold build stage: ULC airport traffic (register PL-ENV-026), quarterly observations of silver.ulc_airports_traffic.
   Block 'airport_traffic': measures passengers and passenger flight operations (frequency Q only). An airport is
   the series breakdown ({"airport": <ICAO>}, is_total false) in the geography of its voivodeship; the tables' total
   row ('_T', all listed airports) is the Poland series (is_total true). Values and units as published. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'ulc_airports'
),

airports as (
    select
        airport_code,
        airport_name
    from ({{ poc_ulc_airports_list() }})
)

select
    'ulc_airports' as source_system,
    'airport_traffic' as block_id,
    'ULC airport traffic: passengers and passenger flight operations (quarterly)' as block_name,
    'ulc_airports:airport_traffic:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'passengers' then 'Passengers handled, regular and charter traffic (ULC, quarterly)'
        when 'passenger_operations' then 'Passenger flight operations, regular and charter traffic (ULC, quarterly)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'passengers' then 'passengers' when 'operations' then 'flight operations' end as unit_name,
    s.airport_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'Q' as frequency,
    s.period_date as date_key,
    cast(s.period_year as varchar) || '-Q' || cast(s.period_quarter as varchar) as period_label,
    case
        when s.airport_code = '_T' then '{}'
        else cast(to_json(map(['airport'], [s.airport_code])) as varchar)
    end as breakdown_json,
    case
        when s.airport_code = '_T' then cast(null as varchar)
        else cast(to_json(map(['airport'], [a.airport_name])) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    s.airport_code = '_T' as is_total,
    cast(s.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('ulc_airports_traffic') }} as s
left join geo as g on s.airport_code = g.source_code
left join airports as a on s.airport_code = a.airport_code
