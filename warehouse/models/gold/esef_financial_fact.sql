{{ config(alias='esef_financial_fact') }}
{# Gold entity-level facts: revenue (ifrs-full:Revenue) and profit or loss (ifrs-full:ProfitLoss) of the reporting
   period as tagged in the ESEF annual reports of Polish issuers held by filings.xbrl.org, one row per filing
   (fxo_id), concept and unit, in the currency published (unit_code as tagged, e.g. iso4217:PLN; currency_code
   beside it), never converted and never summed into a national total. Only entity-level facts (no dimension beyond
   concept, entity, period, unit) of the period that ends on the filing's period end. A filing whose concept has
   several candidates is left out, never picked from: several values for one period, or several periods ending on
   the period end (tests/xbrl_filings_fact_conflicts.sql warns and lists them). Comparative periods stay in Silver.
   The grain stays at the filing: language versions and refilings of one report each have their rows, because the
   repository has no version column that tells them apart (tests/esef_issuer_period_values_differ.sql warns when
   they disagree). A missing fact is no row. Joins gold.esef_filing by fxo_id and GLEIF by lei.
   Source: silver.xbrl_filings_facts. #}
with facts as (select * from {{ ref('xbrl_filings_facts') }})

select
    fxo_id,
    lei,
    concept,
    case concept when 'ifrs-full:Revenue' then 'revenue' when 'ifrs-full:ProfitLoss' then 'profit_loss' end as measure,
    period_start,
    period_end,
    year(period_end) as period_end_year,
    unit_code,
    case when unit_code like 'iso4217:%' then substr(unit_code, 9) end as currency_code,
    value,
    decimals,
    facts,
    source_version,
    raw_sha256
from facts
where is_reporting_period and reporting_period_candidates = 1 and values_distinct = 1
