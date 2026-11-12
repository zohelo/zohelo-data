{{ config(severity='warn') }}
{# silver.xbrl_filings_facts: one filing states several different values for one concept, period and unit without
   any dimension (for example the profit of the income statement and another figure tagged with the same concept).
   Nothing is picked: the row keeps value null with the range, and gold.esef_financial_fact leaves it out. One row
   per such key (warn). #}
select
    fxo_id,
    concept,
    period_start,
    period_end,
    unit_code,
    values_distinct,
    value_min,
    value_max
from {{ ref('xbrl_filings_facts') }}
where values_distinct > 1
