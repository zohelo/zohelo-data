{{ config(alias='build_nbp') }}
{# Gold PoC build stage: NBP exchange rates (table A mid; table C bid and ask) and gold
   fixings as daily observations. The currency is the series breakdown, the geography is
   not applicable (_Z). #}
with rates as (
    select
        'nbp:fx:' || table_code || ':' || rate_type as measure_key,
        table_code,
        rate_type,
        effective_date,
        currency_code,
        currency_name,
        rate,
        source_version,
        raw_sha256
    from (
        select
            table_code,
            effective_date,
            currency_code,
            currency_name,
            source_version,
            raw_sha256,
            unnest(['mid', 'bid', 'ask']) as rate_type,
            unnest([mid_rate, bid_rate, ask_rate]) as rate
        from {{ ref('nbp_fx_rates') }}
        where table_code in ('A', 'C')
    )
    where rate is not null
)

select
    'nbp' as source_system,
    'fx_' || table_code as block_id,
    'NBP exchange rates, table ' || table_code as block_name,
    measure_key,
    'Exchange rate, NBP table ' || table_code || ', ' || rate_type || ' (PLN per 1 unit)' as measure_name,
    rate_type as measure_codes,
    'PLN' as unit_code,
    'PLN per 1 unit of the currency' as unit_name,
    cast(null as varchar) as geography_source_code,
    '_Z' as geography_key,
    'D' as frequency,
    effective_date as date_key,
    cast(effective_date as varchar) as period_label,
    to_json(map(['currency'], [currency_code])) as breakdown_json,
    to_json(map(['currency'], [currency_name])) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(rate as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from rates
union all
select
    'nbp' as source_system,
    'gold' as block_id,
    'NBP gold fixings' as block_name,
    'nbp:gold:price' as measure_key,
    'Gold price, NBP fixing (PLN per gram, 1000 fineness)' as measure_name,
    'price' as measure_codes,
    'PLN/g' as unit_code,
    'PLN per gram' as unit_name,
    null as geography_source_code,
    '_Z' as geography_key,
    'D' as frequency,
    effective_date as date_key,
    cast(effective_date as varchar) as period_label,
    '{}' as breakdown_json,
    null as breakdown_label_json,
    null as sex_code,
    true as is_total,
    cast(price_pln_per_gram as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from {{ ref('nbp_gold_fixings') }}
where price_pln_per_gram is not null
