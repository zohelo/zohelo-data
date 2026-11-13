-- kprm_work_list (PLG-034): the Silver table holds drafts, and each draft's kind agrees with the first letter of its
-- work-list number (U/R/I = Projekty ustaw / rozporządzeń / innych dokumentów rządowych): columns that shifted
-- fail here instead of loading silently. The type (Typ dokumentu) is not compared with the number's later letters:
-- they disagree in 2 of 1,526 drafts (UC57 and UDER84 are typed D, Dev trial 43343159781, 13 Nov 2026; a number is
-- not reissued when a draft's type is reclassified, as far as the file shows); the type's accepted_values catch a
-- shifted column. No row floor: a new government starts a new, short list. One test per Silver model (it reads no
-- other source's table).
with drafts as (
    select
        draft_number,
        kind,
        left(draft_number_prefix, 1) as kind_letter
    from {{ ref('kprm_work_list_drafts') }}
),

mismatched as (
    select
        draft_number,
        kind,
        'kind disagrees with the number' as problem
    from drafts
    where
        kind_letter not in ('U', 'R', 'I')
        or kind
        <> case kind_letter
            when 'U' then 'Projekty ustaw'
            when 'R' then 'Projekty rozporządzeń'
            when 'I' then 'Projekty innych dokumentów rządowych'
        end
)

select * from mismatched
union all
select
    null as draft_number,
    null as kind,
    'no drafts' as problem
where not exists (select 1 from drafts)
