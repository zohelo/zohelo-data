{{ config(alias='build_gus_health', tags=['gold_block']) }}
{# Gold build stage: GUS health and health care tables (annual) (register PLS-004, health family), annual
   observations of silver.gus_health_tables. Block 'health_care_resources': one measure_key per measure (frequency A
   only); the area is the voivodeship name or 'polska', mapped by poc_geography__gus_health (TERYT names). Stocks
   (doctors, nurses, hospitals, beds, beds per 10,000) are as of 31 December, inpatients are a count of the year.
   Rows without a value (GUS '.', '#', 'x') are not loaded; a stored 0 (less than half a unit) is 'flagged'. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_health'
)

select
    'gus_health' as source_system,
    'health_care_resources' as block_id,
    'GUS health and health care tables (annual)' as block_name,
    'gus_health:health_care_resources:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'doctors' then 'Doctors working with patients, 31 December (persons)'
        when 'nurses' then 'Nurses working with patients, 31 December (persons)'
        when 'hospitals' then 'General hospitals, 31 December (facilities)'
        when 'beds' then 'Beds in general hospitals, 31 December (beds)'
        when 'beds_per_10k' then 'Beds in general hospitals per 10,000 population, 31 December'
        when 'inpatients' then 'Inpatients treated in general hospitals in the year (persons)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'persons' then 'persons'
        when 'facilities' then 'facilities'
        when 'beds' then 'beds'
        when 'per_10k_population' then 'per 10,000 population'
    end as unit_name,
    s.area_name as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.period_year, 1, 1) as date_key,
    cast(s.period_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    case s.cell_status
        when 'low_reliability' then 'low_reliability'
        when 'below_half_unit' then 'flagged'
        else 'ok'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('gus_health_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
