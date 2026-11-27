{{ config(alias='build_ure_prices', tags=['gold_block']) }}
{# Gold build stage: URE energy price indicators (register PL-ENV-020, #2870) from silver.ure_prices_indicators,
   Poland. Block 'energy_prices'. One measure_key per indicator x frequency x unit
   (ure_prices:energy_prices:<unit>:<indicator>.<annual|quarterly>), so the yearly and the quarterly competitive-market
   price are separate series and never meet on one date_key; the gas price printed in PLN/m3 (2013-2014) is its own
   measure beside the PLN/MWh one. date_key = first day of the year or quarter. Values and units as published (D33):
   PLN/MWh and PLN/kWh are not rescaled. Rows without a value (value_status 'empty' or 'dash') are not loaded. The
   area 'PL' is mapped in poc_geography__ure_prices. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'ure_prices'
)

select
    'ure_prices' as source_system,
    'energy_prices' as block_id,
    'URE energy price indicators' as block_name,
    'ure_prices:energy_prices:' || s.unit_code || ':' || s.indicator_code || '.'
    || case s.frequency when 'A' then 'annual' when 'Q' then 'quarterly' end as measure_key,
    case s.indicator_code
        when 'electricity_competitive_market'
            then case s.frequency
                when 'A' then 'Average yearly electricity sale price on the competitive market'
                when 'Q' then 'Average quarterly electricity sale price on the competitive market'
            end
        when 'electricity_households'
            then 'Average household electricity price, distribution charge and excise included, VAT excluded'
        when 'gas_import_eu_efta' then 'Average purchase price of natural gas imported from EU or EFTA member states'
        when 'gas_import_non_eu_efta'
            then 'Average purchase price of natural gas imported from countries other than EU or EFTA members'
    end as measure_name,
    s.indicator_code || '.' || case s.frequency when 'A' then 'annual' when 'Q' then 'quarterly' end as measure_codes,
    s.unit_code,
    s.unit_published as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    s.frequency,
    s.period_start as date_key,
    s.period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('ure_prices_indicators') }} as s
left join geo as g on g.source_code = 'PL'
where s.value_status = 'ok' and s.value is not null
