{# GPW EBI issuer reports (register CD-007): one row per EBI report in GPW's public report list
   (https://www.gpw.pl/espi-ebi-reports, "EBI" rows only; ESPI rows are CD-006, knf_espi), from
   bronze.gpw_ebi_reports, the newest landed copy of each day. Grain: report_id (GPW geru_id). Typed:
   published_at (local Warsaw time as listed, no time zone) and published_date; category as listed (Current,
   Quarterly, …); report_number as listed (e.g. '2/2026', '/2026' when the issuer gave no number); isin when the
   list gives one. Titles are not stored (they can name natural persons). #}
with newest as (
    select *
    from {{ source('gpw_ebi', 'gpw_ebi_reports') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
)

select
    report_id,
    strptime(published_text, '%d-%m-%Y %H:%M:%S') as published_at,
    cast(strptime(published_text, '%d-%m-%Y %H:%M:%S') as date) as published_date,
    category,
    nullif(report_number, '') as report_number,
    issuer_name,
    nullif(isin, '') as isin,
    link,
    entity,
    source_version,
    raw_sha256,
    ingested_at
from newest
