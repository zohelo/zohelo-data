{{ config(alias='esef_filing') }}
{# Gold entity table (register source, playbook section 7): ESEF annual financial reports of Polish issuers held
   by filings.xbrl.org (XBRL International), one row per filing, keyed by the repository's filing id (fxo_id).
   Every filing is kept: language versions and refilings of one report are separate filings, told apart only by
   filing_sequence, which is not a version (filings_for_period counts the filings of the issuer and period end).
   lei is the issuer's LEI (joins GLEIF by LEI, never by name). The repository says it is not complete.
   Source: silver.xbrl_filings_filings. #}
with f as (select * from {{ ref('xbrl_filings_filings') }})

select
    fxo_id,
    lei,
    issuer_name,
    period_end,
    year(period_end) as period_end_year,
    country_code,
    report_standard,
    filing_sequence,
    filings_for_period,
    date_added,
    processed_at,
    error_count,
    warning_count,
    inconsistency_count,
    has_json,
    package_url,
    package_sha256,
    source_version,
    raw_sha256
from f
