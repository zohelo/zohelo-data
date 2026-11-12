{{ config(alias='esef_financial_fact') }}
{# Gold entity-level facts: revenue (ifrs-full:Revenue) and profit or loss (ifrs-full:ProfitLoss) as tagged in
   the ESEF annual reports of Polish issuers held by filings.xbrl.org, one row per filing (fxo_id), concept, period
   and unit, in the currency published (unit_code as tagged, e.g. iso4217:PLN; currency_code beside it), never
   converted and never summed into a national total. Only entity-level facts (no dimension beyond concept, entity,
   period, unit) with one value in the filing; a filing that states several values for a key is left out
   (tests/xbrl_filings_fact_conflicts.sql). The grain stays at the filing: language versions and refilings of one
   report each have their rows (tests/esef_issuer_period_values_differ.sql warns when they disagree), and a
   filing also carries its comparative periods (is_reporting_period = false). Joins gold.esef_filing by fxo_id and
   GLEIF by lei. Source: silver.xbrl_filings_facts. #}
with facts as (select * from {{ ref('xbrl_filings_facts') }})

select
    fxo_id,
    lei,
    concept,
    case concept when 'ifrs-full:Revenue' then 'revenue' when 'ifrs-full:ProfitLoss' then 'profit_loss' end as measure,
    period_start,
    period_end,
    is_reporting_period,
    unit_code,
    case when unit_code like 'iso4217:%' then substr(unit_code, 9) end as currency_code,
    value,
    decimals,
    facts,
    source_version,
    raw_sha256
from facts
where values_distinct = 1
