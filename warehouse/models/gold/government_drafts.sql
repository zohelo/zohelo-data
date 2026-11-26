{{ config(alias='government_drafts') }}
{# Gold entity table (register source, kpi false; docs/model.md "Fact kinds": documents): one row per government
   draft on the work list of the Council of Ministers (register PLG-034, #491), keyed by the work-list number
   (Numer projektu); date_key = the day it was published on the list (Data publikacji). Kind, type, bodies, status
   and the planned adoption as published (planned_adoption_text is a plan in free text, not a date). No texts of the
   drafts and no personal data. #}
select
    draft_number,
    number_prefix,
    published_on as date_key,
    published_at,
    title,
    document_kind,
    document_type,
    drafting_body,
    cooperating_body,
    responsible_body,
    planned_adoption_text,
    status_key,
    status_text,
    withdrawal_info,
    additional_info,
    preview_url,
    'kprm_work_list' as source_system,
    source_version,
    raw_sha256
from {{ ref('kprm_work_list_drafts') }}
