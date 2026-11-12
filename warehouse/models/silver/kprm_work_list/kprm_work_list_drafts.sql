{# KPRM work list (register PLG-034, bronze.kprm_work_list_drafts): one row per government draft on the Council of
   Ministers' work list (Wykaz prac legislacyjnych i programowych Rady Ministrów), from the newest Landing copy of the
   file. Grain: draft_number (Numer projektu, unique in the file: a repeat fails the unique test, it is not removed).
   Values as published: kind (Rodzaj dokumentu), type (Typ dokumentu), the responsible, drafting and cooperating
   bodies, planned adoption by the Council of Ministers as free text (a plan, never a date), resignation information.
   status = realised (Zrealizowany), withdrawn (Wycofany) or in_progress (the source leaves the cell empty while the
   draft is being worked on: an explicit state, not unknown); another published status fails the build.
   published_at = Data publikacji, the entry's publication on the list (Warsaw local time as published; it rises with
   the work-list number and does not move when the status changes, probe 43320804384). #}
with newest as (
    select *
    from {{ source('kprm_work_list', 'kprm_work_list_drafts') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    numer_projektu as draft_number,
    regexp_extract(numer_projektu, '^[A-Z]+') as draft_number_prefix,
    tytul as title,
    rodzaj_dokumentu as kind,
    typ_dokumentu as document_type,
    informacja_dodatkowa as additional_info,
    organ_odpowiedzialny as responsible_body,
    organ_opracowujacy as drafting_body,
    organ_wspolpracujacy as cooperating_body,
    planowane_przyjecie as planned_adoption,
    informacja_o_rezygnacji as resignation_info,
    status_realizacji as status_published,
    case
        when status_realizacji is null then 'in_progress'
        when status_realizacji = 'Zrealizowany' then 'realised'
        when status_realizacji = 'Wycofany' then 'withdrawn'
        else error('kprm_work_list_drafts: unknown Status realizacji ' || status_realizacji)
    end as status,
    strptime(data_publikacji, '%Y-%m-%d %H:%M') as published_at,
    podglad as link,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
