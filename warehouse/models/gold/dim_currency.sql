{# Conformed currency (warehouse/dimensions/currency.yaml), v1 from NBP: every ISO 4217
   code NBP has quoted, its latest Polish name and quotation history, plus PLN and the
   special members. ISO 4217 names/minor units follow when the ISO list is ingested. #}
with quoted as (
    select currency_code,
           arg_max(currency_name, effective_date) as name_pl,
           list(distinct table_code order by table_code) as nbp_tables,
           min(effective_date) as first_nbp_quote,
           max(effective_date) as last_nbp_quote
    from {{ ref('nbp_fx_rates') }}
    group by currency_code
)
select currency_code, name_pl, nbp_tables, first_nbp_quote, last_nbp_quote, 'currency' as member_type
from quoted
union all
select 'PLN', 'złoty polski', [], null, null, 'currency'
where not exists (select 1 from quoted where currency_code = 'PLN')
union all select '_Z', 'nie dotyczy (not monetary)', [], null, null, 'not_applicable'
union all select '_X', 'inna / nieokreślona (other or unspecified)', [], null, null, 'unknown'
