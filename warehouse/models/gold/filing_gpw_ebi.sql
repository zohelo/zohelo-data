{{ config(alias='filing_gpw_ebi') }}
{# Gold event table (filings, docs/model.md "Event / transaction"): one row per EBI report in GPW's report list,
   keyed by the source's own report id (geru_id), from silver.gpw_ebi_reports, with the issuer's ISIN where the list
   gives it (the conformed security key). The list gives no KRS, NIP, REGON or LEI. No titles (they can name
   natural persons). #}
select
    report_id,
    published_at,
    published_date,
    category,
    report_number,
    issuer_name,
    isin,
    link,
    source_version,
    raw_sha256
from {{ ref('gpw_ebi_reports') }}
