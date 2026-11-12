{# filings.xbrl.org (XBRL International), ESEF annual financial reports of Polish issuers
   (bronze.xbrl_filings_filings): the newest copy of the filing list, typed. Grain: fxo_id (the repository's
   filing id: LEI, period end, report standard, country, sequence). The sequence (last fxo_id segment) only
   numbers the filings the repository holds for one issuer and period end: language versions and refilings both
   get the next number, so it is kept as published and never read as a version; filings_for_period says how many
   filings share the issuer and period end. Timestamps as published (no zone given). Source codes kept. #}
with newest as (
    select *
    from {{ source('xbrl_filings', 'xbrl_filings_filings') }}
    qualify ingested_at = max(ingested_at) over (partition by entity)
)

select
    fxo_id,
    entity_identifier as lei,
    entity_name as issuer_name,
    cast(period_end as date) as period_end,
    country as country_code,
    regexp_extract(fxo_id, '^[A-Z0-9]{20}-[0-9]{4}-[0-9]{2}-[0-9]{2}-([A-Za-z0-9]+)-', 1) as report_standard,
    cast(regexp_extract(fxo_id, '-([0-9]+)$', 1) as integer) as filing_sequence,
    count(*) over (partition by entity_identifier, period_end) as filings_for_period,
    cast(date_added as timestamp) as date_added,
    cast(processed as timestamp) as processed_at,
    cast(error_count as integer) as error_count,
    cast(warning_count as integer) as warning_count,
    cast(inconsistency_count as integer) as inconsistency_count,
    sha256 as package_sha256,
    package_url,
    report_url,
    viewer_url,
    json_url,
    json_url is not null as has_json,
    cast(filing_id as bigint) as filing_id,
    source_version,
    raw_sha256,
    ingested_at
from newest
