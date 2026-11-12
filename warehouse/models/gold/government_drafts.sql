{{ config(alias='government_drafts') }}
{# Gold entity table (register source, kpi false; docs/model.md "Three kinds of dimensions", entity: documents): one
   row per government draft on the Council of Ministers' work list (KPRM, CC BY-SA 4.0), keyed by the official
   work-list number (Numer projektu, e.g. UD505), which RPL shows for the same project. Latest version only
   (policy #822/#845): attributes as the newest file publishes them, no validity history. planned_adoption is the
   published plan as free text, not a date; status in_progress = the source's empty Status realizacji. Event counts
   by entry month are in gold.build_kprm_work_list. #}
select
    draft_number,
    draft_number_prefix,
    title,
    kind,
    document_type,
    additional_info,
    responsible_body,
    drafting_body,
    cooperating_body,
    planned_adoption,
    status,
    status_published,
    resignation_info,
    published_at,
    cast(published_at as date) as published_date,
    link,
    'kprm_work_list' as source_system,
    source_version,
    raw_sha256
from {{ ref('kprm_work_list_drafts') }}
