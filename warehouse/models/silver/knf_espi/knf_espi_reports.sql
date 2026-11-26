{# KNF ESPI issuer reports (register CD-006): one row per report in KNF's public OAM list
   (https://moam.knf.gov.pl/moam.nsf, view by day of receipt), from bronze.knf_espi_reports, the newest landed copy
   of each day. Grain: report_id (the OAM document id). Typed: received_date; report_type is the ESPI form code that
   starts the listed title (RB current report, RB-W, *_ASO alternative trading system, QSr/Q/PSr/P/RS/R periodic
   reports, UNI-EN English version, …), '_U' when the title starts with none. Titles are not stored (they can name
   natural persons). listed_for_day = the number of reports the list states for the day (null: a day of one page).
   No issuer identifiers: the list gives only the issuer's name. #}
with newest as (
    select *
    from {{ source('knf_espi', 'knf_espi_reports') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
)

select
    report_id,
    issuer_name,
    cast(received_date as date) as received_date,
    coalesce(nullif(report_type, ''), '_U') as report_type,
    link,
    listed as listed_for_day,
    entity,
    source_version,
    raw_sha256,
    ingested_at
from newest
