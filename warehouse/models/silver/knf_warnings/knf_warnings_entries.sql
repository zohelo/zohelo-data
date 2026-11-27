{# KNF public warning list (register PLG-019): one row per entry of every landed copy of the list page
   (https://www.knf.gov.pl/dla_konsumenta/ostrzezenia_publiczne), from bronze.knf_warnings_entries. The page shows only
   the current list, so every copy is kept (an accumulating snapshot; history starts with the first copy). Grain:
   source_version x section_key x lp. Typed: snapshot_date (the download date), lp, final_ruling_mentioned (boolean),
   krs_count. prosecutor_office as listed (first office; a note in parentheses, such as a case handed to another
   office, is kept in prosecutor_office_listed); prosecutor_level from the office's name (krajowa, regionalna,
   okregowa, rejonowa, other). Entries are notifications of a suspected offence filed by the KNF, not findings of
   guilt. No names and no free text (reduced before Landing). #}
with entries as (
    select
        source_version,
        cast(ingested_at as date) as snapshot_date,
        section_key,
        section_position,
        section_title,
        section_entries,
        cast(lp as integer) as lp,
        identifier_kind,
        nullif(krs_numbers, '') as krs_numbers,
        trim(prosecutor_office) as prosecutor_office_listed,
        trim(regexp_replace(prosecutor_office, '\s*\(.*$', '')) as prosecutor_office,
        final_ruling_mentioned = 'true' as final_ruling_mentioned,
        stated_total,
        source_row_number,
        entity,
        raw_sha256,
        ingested_at
    from {{ source('knf_warnings', 'knf_warnings_entries') }}
)

select
    source_version,
    snapshot_date,
    section_key,
    section_position,
    section_title,
    section_entries,
    lp,
    identifier_kind,
    krs_numbers,
    coalesce(len(string_split(krs_numbers, ' ')), 0) as krs_count,
    prosecutor_office,
    prosecutor_office_listed,
    case
        when prosecutor_office like 'Prokuratura Krajowa%' then 'krajowa'
        when prosecutor_office like 'Prokuratura Regionalna%' then 'regionalna'
        when prosecutor_office like 'Prokuratura Okręgowa%' then 'okregowa'
        when prosecutor_office like 'Prokuratura Rejonowa%' then 'rejonowa'
        else 'other'
    end as prosecutor_level,
    final_ruling_mentioned,
    stated_total,
    source_row_number,
    entity,
    raw_sha256,
    ingested_at
from entries
