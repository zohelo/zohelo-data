{{ config(alias='build_mrpips_unemployment') }}
{# Gold build stage: MRPiPS registered unemployment (register PLS-029) as monthly observations.
   One block, 'registered_unemployment': measures unemployed_persons (persons, by sex; Poland and voivodeships),
   unemployed_thousands (thousand persons) and unemployment_rate (%) (both sexes; Poland, voivodeships, powiats).
   The area is the TERYT code of the rate workbook (WWPP; 0000 = Poland), mapped in poc_map_geography. #}
with geo as (
    select source_code, geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'mrpips_unemployment'
)

select
    'mrpips_unemployment' as source_system,
    'registered_unemployment' as block_id,
    'MRPiPS registered unemployment (end of month)' as block_name,
    'mrpips_unemployment:registered_unemployment:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'unemployed_persons' then 'Registered unemployed persons, end of month (MRPiPS-01)'
        when 'unemployed_thousands' then 'Registered unemployed, end of month (thousand persons)'
        when 'unemployment_rate' then 'Registered unemployment rate, end of month (%)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'persons' then 'persons' when 'thousand_persons' then 'thousand persons' else '%' end
        as unit_name,
    s.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    s.period_date as date_key,
    strftime(s.period_date, '%Y-%m') as period_label,
    cast(to_json(map(['sex'], [s.sex_code])) as varchar) as breakdown_json,
    cast(to_json(map(['sex'], [case s.sex_code when 'T' then 'ogółem' when 'F' then 'kobiety' else 'mężczyźni' end]))
        as varchar) as breakdown_label_json,
    s.sex_code,
    s.sex_code = 'T' as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('mrpips_registered_unemployment') }} as s
left join geo as g on s.area_code = g.source_code
