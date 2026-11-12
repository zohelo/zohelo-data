{{ config(severity='warn') }}
{# gold.esef_financial_fact: several filings of one issuer and period end (language versions, refilings) state
   different values for one concept, reporting period and unit. The table keeps every filing (grain fxo_id), so
   nothing is hidden or picked; a reader who needs one value per issuer and period must choose the filing. One
   row per issuer, concept, period and unit with more than one value (warn). #}
select
    lei,
    concept,
    period_start,
    period_end,
    unit_code,
    count(*) as filings,
    count(distinct value) as values_distinct,
    min(value) as value_min,
    max(value) as value_max,
    string_agg(fxo_id, ',' order by fxo_id) as fxo_ids
from {{ ref('esef_financial_fact') }}
group by lei, concept, period_start, period_end, unit_code
having count(distinct value) > 1
