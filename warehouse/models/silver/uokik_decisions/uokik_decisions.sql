{# Decisions of the President of UOKiK (register PLG-020; concentration decisions PLG-021): one row per decision of
   the database's view WWW-wszystkie (https://decyzje.uokik.gov.pl/bp/dec_prez.nsf), from bronze.uokik_decisions_index,
   the newest landed copy of each subject-area category. Grain: unid x category_key (the view entry's @unid in each
   subject-area category that lists it: a Domino categorised view lists a document once per value of a multi-valued
   category, so one decision can sit in two categories; gold.uokik_decision is one row per unid). Typed: decision_date
   (as the cell gives it: YYYY-MM-DD, DD.MM.YYYY or YYYY.MM.DD; null when the cell gives none), decision_year (from
   the date, else the view's year category). department_code = the letters that start the decision number (the
   signature of the department or branch office that issued it, e.g. DOZIK, DKK, RWA); the cell's free-text labels
   (department, parties) are not kept in Landing (#3330 review). is_concentration: the number's department code is
   DKK (Departament Kontroli Koncentracji, PLG-021). Source codes kept as listed. No party names (they can be natural persons). #}
with newest as (
    select *
    from {{ source('uokik_decisions', 'uokik_decisions_index') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
),

typed as (
    select
        upper(unid) as unid,
        nullif(noteid, '') as noteid,
        trim(decision_number) as decision_number,
        upper(regexp_extract(trim(decision_number), '^([A-Za-zĄĆĘŁŃÓŚŹŻ]+)', 1)) as department_code,
        coalesce(
            try_strptime(nullif(trim(decision_date), ''), '%Y-%m-%d'),
            try_strptime(nullif(trim(decision_date), ''), '%d.%m.%Y'),
            try_strptime(nullif(trim(decision_date), ''), '%Y.%m.%d'),
            try_strptime(nullif(trim(decision_date), ''), '%d-%m-%Y')
        )::date as decision_date,
        nullif(trim(decision_date), '') as decision_date_text,
        category,
        entity as category_key,
        year_category,
        descendants as category_descendants,
        source_row_number,
        entity,
        source_version,
        raw_sha256,
        ingested_at
    from newest
)

select
    unid,
    noteid,
    decision_number,
    department_code,
    decision_date,
    decision_date_text,
    coalesce(year(decision_date), try_cast(year_category as integer)) as decision_year,
    category,
    category_key,
    year_category,
    department_code = 'DKK' as is_concentration,
    'https://decyzje.uokik.gov.pl/bp/dec_prez.nsf/WWW-wszystkie/' || unid || '?OpenDocument' as link,
    category_descendants,
    source_row_number,
    entity,
    source_version,
    raw_sha256,
    ingested_at
from typed
