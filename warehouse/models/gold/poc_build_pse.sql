{{ config(alias='build_pse', tags=['gold_block']) }}
{# Gold build stage: PSE Reports API v2 (register PL-ENV-019) as daily observations for Poland (fact_daily), from
   silver.pse_daily (mean of the published 15-minute values over each complete business day; incomplete days are not
   in it). Blocks = PSE reports: 'rce_pln' (measure rce_pln, PLN/MWh) and 'his_wlk_cal' (measures demand, wi = wind,
   pv = photovoltaics, MW). measure_key pse:<block>:<unit>:<PSE code>; values and units as Silver holds them, no
   rescaling. The area is the whole national power system: source code 'PL', mapped in poc_map_geography. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'pse'
)

select
    'pse' as source_system,
    replace(s.report, '-', '_') as block_id,
    case s.report
        when 'rce-pln' then 'PSE market price of electricity RCE (daily mean)'
        else 'PSE power system: demand, wind and photovoltaic generation (daily mean)'
    end as block_name,
    'pse:' || replace(s.report, '-', '_') || ':' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'rce_pln' then 'Market price of electricity RCE, daily mean of 15-minute prices (PLN/MWh)'
        when 'demand' then 'Power demand of the national power system, daily mean (MW)'
        when 'wi' then 'Wind generation, daily mean (MW)'
        when 'pv' then 'Photovoltaic generation, daily mean (MW)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'pln_per_mwh' then 'PLN per MWh' else 'MW' end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    s.business_date as date_key,
    cast(s.business_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('pse_daily') }} as s
left join geo as g on g.source_code = 'PL'
