{{ config(alias='build_ure', tags=['gold_block']) }}
{# Gold build stage: URE electricity price indicators (register PL-ENV-020), Poland only. One block per indicator
   page: 'competitive_market_price' (page 7852, zł/MWh; yearly and quarterly averages as two measures, one frequency
   each, so the yearly and the first-quarter value never share a series and date) and 'household_price' (page 7853,
   zł/kWh, yearly). measure_name carries URE's column header (the indicator's definition as published); values and
   units as published. The country is the literal source code 'PL' (poc_map_geography section ure). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'ure'
)

select
    'ure' as source_system,
    s.indicator_code as block_id,
    case s.indicator_code
        when 'competitive_market_price' then 'URE average electricity sale price on the competitive market'
        when 'household_price' then 'URE average electricity price for households'
    end as block_name,
    'ure:' || s.indicator_code || ':' || s.unit_code || ':' || case s.frequency when 'A' then 'annual' else 'quarterly' end
        as measure_key,
    s.indicator_name || ' (URE, ' || case s.frequency when 'A' then 'yearly' else 'quarterly' end || ')'
        as measure_name,
    case s.frequency when 'A' then 'annual' else 'quarterly' end as measure_codes,
    s.unit_code,
    s.unit_text as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    s.frequency,
    s.period_start as date_key,
    s.period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('ure_price_indicators') }} as s
left join geo as g on g.source_code = 'PL'
where s.observation_status = 'ok' and s.value is not null
