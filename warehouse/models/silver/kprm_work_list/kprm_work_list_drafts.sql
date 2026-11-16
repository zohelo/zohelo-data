{# Work list of the Council of Ministers (Wykaz prac legislacyjnych i programowych Rady Ministrów; register PLG-034,
   #491): one row per government draft on the list, from the newest landed copy of the work-list CSV
   (bronze.kprm_work_list_drafts). Grain: draft_number (Numer projektu, the work-list number, e.g. UD505, UC2).
   Text as published (trimmed; an empty cell is null): title, kind, type, bodies, the planned adoption as free text
   (a plan, never turned into a date or a measured value), the withdrawal note. status_key: 'realised'
   (Zrealizowany), 'withdrawn' (Wycofany), 'in_progress' (no status: the draft is still being worked on, not
   unknown); any other status text fails the build. published_at: Data publikacji ('YYYY-MM-DD HH:MM', Warsaw
   time); any other form fails the build. Not here: the aims and substance texts and the responsible official. #}
with newest as (
    select *
    from {{ source('kprm_work_list', 'kprm_work_list_drafts') }}
    qualify rank() over (order by source_version desc, ingested_at desc) = 1
),

cleaned as (
    select
        nullif(trim(draft_number), '') as draft_number,
        nullif(trim(title), '') as title,
        nullif(trim(document_kind), '') as document_kind,
        nullif(trim(document_type), '') as document_type,
        nullif(trim(additional_info), '') as additional_info,
        nullif(trim(drafting_body), '') as drafting_body,
        nullif(trim(cooperating_body), '') as cooperating_body,
        nullif(trim(responsible_body), '') as responsible_body,
        nullif(trim(planned_adoption), '') as planned_adoption_text,
        nullif(trim(withdrawal_info), '') as withdrawal_info,
        nullif(trim(status_text), '') as status_text,
        nullif(trim(published_at), '') as published_at_text,
        nullif(trim(preview_url), '') as preview_url,
        source_row_number,
        source_version,
        raw_sha256
    from newest
)

select
    draft_number,
    regexp_extract(draft_number, '^[^0-9]+') as number_prefix,
    title,
    document_kind,
    document_type,
    additional_info,
    drafting_body,
    cooperating_body,
    responsible_body,
    planned_adoption_text,
    withdrawal_info,
    status_text,
    case
        when status_text is null then 'in_progress'
        when status_text = 'Zrealizowany' then 'realised'
        when status_text = 'Wycofany' then 'withdrawn'
        else error('kprm_work_list_drafts: unknown Status realizacji: ' || status_text)
    end as status_key,
    case
        when regexp_full_match(published_at_text, '\d{4}-\d{2}-\d{2} \d{2}:\d{2}')
            then strptime(published_at_text, '%Y-%m-%d %H:%M')
        else error('kprm_work_list_drafts: Data publikacji not YYYY-MM-DD HH:MM: ' || coalesce(published_at_text, 'null'))
    end as published_at,
    cast(left(published_at_text, 10) as date) as published_on,
    preview_url,
    source_row_number,
    source_version,
    raw_sha256
from cleaned
