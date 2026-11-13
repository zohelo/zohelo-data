{{ config(alias='build_cepik', tags=['gold_block']) }}
{# Gold build stage: CEPiK vehicles registered per month (register PL-ENV-024), monthly observations of
   silver.cepik_vehicle_registrations. Block 'vehicle_registrations', one measure (frequency M only):
   - totals: Poland (the workbook's national row) and the 16 voivodeships, is_total;
   - by vehicle type: Poland, breakdown vehicle_type (the type as printed, lower case), not total.
   Values as published (vehicles). Voivodeship rows of a month marked order_suspect in Silver (values printed in
   another order than the names) are left out; the month's national total stays. Unpublished months are absent in
   Silver, so no row here is a placeholder 0. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'cepik'
),

rows_in as (
    select
        *,
        case when table_code = 'voivodeship' then group_code else 'polska' end as area_code
    from {{ ref('cepik_vehicle_registrations') }}
    where value is not null and check_status = 'ok'
)

select
    'cepik' as source_system,
    'vehicle_registrations' as block_id,
    'CEPiK vehicles registered per month' as block_name,
    'cepik:vehicle_registrations:vehicles' as measure_key,
    'Vehicles registered in the month (CEPiK)' as measure_name,
    'vehicle_registrations' as measure_codes,
    'vehicles' as unit_code,
    'vehicles' as unit_name,
    s.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    s.period_date as date_key,
    strftime(s.period_date, '%Y-%m') as period_label,
    case
        when s.table_code = 'vehicle_type' then cast(to_json(map(['vehicle_type'], [s.group_code])) as varchar)
        else '{}'
    end as breakdown_json,
    case
        when s.table_code = 'vehicle_type' then cast(to_json(map(['vehicle_type'], [s.group_label])) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    s.table_code = 'voivodeship' as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from rows_in as s
left join geo as g on s.area_code = g.source_code
