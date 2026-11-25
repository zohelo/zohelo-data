{{ config(alias='esef_issuer') }}
{# Gold entity table: issuers with at least one ESEF annual financial report held by filings.xbrl.org for Poland,
   one row per LEI (the official identifier; joins GLEIF by LEI, never by name), with the name the repository
   gives and the filings it holds. Grouped by LEI and name, so an LEI with two names would fail the unique test
   instead of being picked. Source: silver.xbrl_filings_filings. #}
with f as (select * from {{ ref('xbrl_filings_filings') }})

select
    lei,
    issuer_name,
    country_code,
    count(*) as filings,
    count(distinct period_end) as period_ends,
    min(period_end) as first_period_end,
    max(period_end) as last_period_end,
    max(date_added) as last_date_added,
    max(source_version) as source_version,
    max(raw_sha256) as raw_sha256
from f
group by lei, issuer_name, country_code
