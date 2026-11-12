-- kprm_work_list (PLG-034): the Silver table holds drafts, and each draft's kind and type agree with the letters of
-- its work-list number (U/R/I = Projekty ustaw / rozporządzeń / innych dokumentów rządowych; then D, C, DER, PRO, A =
-- the type's code). Columns that shifted or a changed code list fail here instead of loading silently. No row floor:
-- a new government starts a new, short list. One test per Silver model (it reads no other source's table).
with drafts as (
    select
        draft_number,
        kind,
        document_type,
        left(draft_number_prefix, 1) as kind_letter,
        substr(draft_number_prefix, 2) as type_code
    from {{ ref('kprm_work_list_drafts') }}
),

mismatched as (
    select
        draft_number,
        kind,
        document_type,
        'kind or type disagrees with the number' as problem
    from drafts
    where
        kind
        <> case kind_letter
            when 'U' then 'Projekty ustaw'
            when 'R' then 'Projekty rozporządzeń'
            when 'I' then 'Projekty innych dokumentów rządowych'
        end
        or split_part(replace(document_type, '–', '-'), ' - ', 1) <> type_code
        or kind_letter not in ('U', 'R', 'I')
)

select * from mismatched
union all
select
    null as draft_number,
    null as kind,
    null as document_type,
    'no drafts' as problem
where not exists (select 1 from drafts)
