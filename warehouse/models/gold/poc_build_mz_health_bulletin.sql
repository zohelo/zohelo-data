{{ config(alias='build_mz_health_bulletin', tags=['gold_block']) }}
{# Gold build stage: MZ / CeZ statistical bulletin (register PLS-017, #2957), annual observations of
   silver.mz_health_bulletin_tables. Block 'health_bulletin': one measure_key per table kind x profession (or
   hospital item) x statistic, unit in the key (frequency A only, no derived frequency). Health workers licensed
   (stock), health workers employed (persons by primary workplace), general hospitals and beds, and hospital activity
   (inpatient cases, patient-days, stays, bed use) stay separate measures. date_key = 1 January of the reference year
   read from the table title; stock dates (31 December) are in the measure name. A reference year printed in two
   editions is taken from the latest edition (is_latest_edition, Silver); the older edition's row stays in Silver.
   Values and units as published (D33): rates per 10,000 population are not rescaled. Rows without a value ('-', '.',
   'x', a sheet repeating an older edition's table) are not loaded. Areas: 'polska' and the 16 voivodeship names, mapped in poc_geography__mz_health_bulletin. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'mz_health_bulletin'
)

select
    'mz_health_bulletin' as source_system,
    'health_bulletin' as block_id,
    'MZ statistical bulletin: health workforce and general hospitals (annual)' as block_name,
    'mz_health_bulletin:health_bulletin:' || s.unit_code || ':' || s.table_kind || '.' || s.profession_code || '.'
    || s.measure_code as measure_key,
    case s.table_kind
        when 'licensed' then 'Licensed to practise, 31 December: '
        when 'employed' then 'Working in health care establishments (persons, primary workplace), 31 December: '
        when 'hospitals' then 'General hospitals: '
        when 'hospital_activity' then 'General hospitals, activity in the year: '
    end
    || case s.profession_code
        when 'doctors' then 'doctors'
        when 'dentists' then 'dentists'
        when 'pharmacists' then 'pharmacists (without pharmacies)'
        when 'lab_diagnosticians' then 'laboratory diagnosticians'
        when 'nurses' then 'nurses'
        when 'midwives' then 'midwives'
        when 'physiotherapists' then 'physiotherapists'
        when 'paramedics' then 'paramedics'
        when 'dietitians' then 'dietitians'
        when 'hospitals' then 'hospitals'
        when 'beds' then 'beds'
        when 'inpatients' then 'inpatients treated'
        when 'patient_days' then 'patient-days'
        when 'length_of_stay' then 'average length of stay'
        when 'bed_occupancy' then 'bed occupancy'
    end
    || case s.measure_code
        when 'per_10k' then ' per 10,000 population'
        when 'per_bed' then ' per bed'
        when 'days' then ' (days in the year)'
        when 'pct' then ' (% of the year)'
        else ''
    end as measure_name,
    s.table_kind || '.' || s.profession_code || '.' || s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'persons' then 'persons'
        when 'per_10k_population' then 'per 10,000 population'
        when 'hospitals' then 'hospitals'
        when 'beds' then 'beds'
        when 'cases' then 'inpatient cases'
        when 'cases_per_bed' then 'inpatient cases per bed'
        when 'thousand_patient_days' then 'thousand patient-days'
        when 'days' then 'days'
        when 'pct' then '%'
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
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('mz_health_bulletin_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.is_latest_edition and s.value is not null
