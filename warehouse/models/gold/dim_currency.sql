{# Conformed currency (warehouse/dimensions/currency.yaml), v1 from NBP: every ISO 4217
   code NBP has quoted, its latest Polish name and quotation history, plus PLN and the
   special members. ISO 4217 names/minor units follow when the ISO list is ingested. #}
with quoted as (
    select
        currency_code,
        arg_max(currency_name, effective_date) as name_pl,
        list(distinct table_code order by table_code) as nbp_tables,
        min(effective_date) as first_nbp_quote,
        max(effective_date) as last_nbp_quote
    from {{ ref('nbp_fx_rates') }}
    group by currency_code
)

select
    currency_code,
    name_pl,
    nbp_tables,
    first_nbp_quote,
    last_nbp_quote,
    'currency' as member_type
from quoted
union all
select
    'PLN' as currency_code,
    'złoty polski' as name_pl,
    [] as nbp_tables,
    null as first_nbp_quote,
    null as last_nbp_quote,
    'currency' as member_type
where
    not exists (
        select 1 from quoted
        where currency_code = 'PLN'
    )
union all
select
    '_Z' as currency_code,
    'nie dotyczy (not monetary)' as name_pl,
    [] as nbp_tables,
    null as first_nbp_quote,
    null as last_nbp_quote,
    'not_applicable' as member_type
union all
select
    '_X' as currency_code,
    'inna / nieokreślona (other or unspecified)' as name_pl,
    [] as nbp_tables,
    null as first_nbp_quote,
    null as last_nbp_quote,
    'unknown' as member_type
