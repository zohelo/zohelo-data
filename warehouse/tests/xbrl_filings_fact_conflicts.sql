{{ config(severity='warn') }}
{# silver.xbrl_filings_facts: a filing with several candidates for one concept and unit in its reporting period.
   Either one period states several different values without any dimension (reason 'several values': for example
   the profit of the income statement and another figure tagged with the same concept), or several periods end on
   the filing's period end (reason 'several periods': a year and a shorter period). Nothing is picked: the rows
   stay in Silver (value null for 'several values') and gold.esef_financial_fact leaves the filing's concept out.
   One row per such key (warn). #}
select
    fxo_id,
    concept,
    period_start,
    period_end,
    unit_code,
    case when values_distinct > 1 then 'several values' else 'several periods' end as reason,
    values_distinct,
    reporting_period_candidates,
    value_min,
    value_max
from {{ ref('xbrl_filings_facts') }}
where is_reporting_period and (values_distinct > 1 or reporting_period_candidates > 1)
