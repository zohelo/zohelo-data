{# filings.xbrl.org (XBRL International), entity-level revenue and profit or loss facts of Polish ESEF annual
   reports (bronze.xbrl_filings_facts, newest version of each filing's reduced xBRL-JSON document), for the filings
   of the current list. Kept: facts of ifrs-full:Revenue and ifrs-full:ProfitLoss without any dimension beyond
   concept, entity, period and unit (no segment, component-of-equity or other axis), with a duration period and a
   numeric value. Grain: fxo_id × concept × period_start × period_end × unit_code. A filing states the same fact
   in several statements: facts with one numeric value become one row (facts = how many); when their values differ,
   value is null and value_min / value_max show the range (values_distinct > 1), never a picked one
   (tests/xbrl_filings_fact_conflicts.sql warns). xBRL-JSON periods end at midnight after the last day:
   period_end is that last day. Values and units as published (iso4217:<currency>), not rescaled; decimals is the
   highest precision stated. is_reporting_period marks the period ending on the filing's own period end (the other
   periods are comparatives); reporting_period_candidates counts the periods of the filing, concept and unit that
   end there (more than one, e.g. a year and a shorter period: Gold leaves the filing's concept out).
   filing_sequence and filings_for_period come from the filing: an issuer and period end with several filings
   (language versions, refilings) has rows for each of them, nothing is picked between filings. #}
with newest as (
    select *
    from {{ source('xbrl_filings', 'xbrl_filings_facts') }}
    qualify ingested_at = max(ingested_at) over (partition by entity)
),

typed as (
    select
        fxo_id,
        concept,
        unit as unit_code,
        cast(cast(split_part(period, '/', 1) as timestamp) as date) as period_start,
        cast(cast(split_part(period, '/', 2) as timestamp) - interval 1 second as date) as period_end,
        try_cast(value as double) as value,
        try_cast(decimals as integer) as decimals,
        source_version,
        raw_sha256
    from newest
    where
        extra_dimensions = '{}'
        and contains(period, '/')
        and unit is not null
        and try_cast(value as double) is not null
),

grouped as (
    select
        fxo_id,
        concept,
        period_start,
        period_end,
        unit_code,
        count(*) as facts,
        count(distinct value) as values_distinct,
        min(value) as value_min,
        max(value) as value_max,
        max(decimals) as decimals,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from typed
    group by fxo_id, concept, period_start, period_end, unit_code
),

joined as (
    select
        g.*,
        f.lei,
        f.filing_sequence,
        f.filings_for_period,
        g.period_end = f.period_end as is_reporting_period
    from grouped as g
    inner join {{ ref('xbrl_filings_filings') }} as f on g.fxo_id = f.fxo_id
)

select
    fxo_id,
    lei,
    filing_sequence,
    filings_for_period,
    concept,
    period_start,
    period_end,
    unit_code,
    case when values_distinct = 1 then value_min end as value,
    value_min,
    value_max,
    values_distinct,
    facts,
    decimals,
    is_reporting_period,
    case
        when is_reporting_period then count(*) over (partition by fxo_id, concept, unit_code, is_reporting_period)
    end as reporting_period_candidates,
    source_version,
    raw_sha256
from joined
