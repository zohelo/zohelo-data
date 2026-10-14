{# Exchange rates, one row per date × currency × rate type × source table: PLN per 1 unit
   of the currency. NBP: table A/B mid, table C bid and ask (mapping_notes in
   warehouse/dimensions/currency.yaml). #}
with rates as (
    select
        effective_date,
        currency_code,
        table_code,
        'mid' as rate_type,
        mid_rate as rate,
        raw_sha256
    from {{ ref('nbp_fx_rates') }}
    where mid_rate is not null
    union all
    select
        effective_date,
        currency_code,
        table_code,
        'bid',
        bid_rate,
        raw_sha256
    from {{ ref('nbp_fx_rates') }}
    where bid_rate is not null
    union all
    select
        effective_date,
        currency_code,
        table_code,
        'ask',
        ask_rate,
        raw_sha256
    from {{ ref('nbp_fx_rates') }}
    where ask_rate is not null
)

select
    cast(strftime(effective_date, '%Y%m%d') as integer) as date_key,
    effective_date as rate_date,
    currency_code,
    'PLN' as quote_currency_code,
    rate_type,
    'nbp' as source_system,
    'table_' || lower(table_code) as source_table,
    rate,
    raw_sha256
from rates
