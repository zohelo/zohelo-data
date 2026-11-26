{{ config(alias='filing_knf_espi') }}
{# Gold event table (filings, docs/model.md "Event / transaction"): one row per ESPI report in KNF's OAM list,
   keyed by the source's own report id (OAM document id), from silver.knf_espi_reports. Issuer by name only: the
   list gives no KRS, NIP, REGON, LEI or ISIN, so the table cannot be joined to gold/silver GLEIF records by an
   identifier (names are never matched, docs/model.md). No titles (they can name natural persons). #}
select
    report_id,
    issuer_name,
    received_date,
    report_type,
    link,
    source_version,
    raw_sha256
from {{ ref('knf_espi_reports') }}
