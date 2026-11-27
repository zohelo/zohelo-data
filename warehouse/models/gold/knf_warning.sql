{{ config(alias='knf_warning') }}
{# Gold entity table (register entries, docs/model.md "Fact kinds"): one row per entry of the newest copy of the KNF
   public warning list (register PLG-019), keyed by section_key x lp (the list's own numbering in each legal-basis
   section, valid for that copy only), from silver.knf_warnings_entries. KRS numbers as listed (space separated) link
   to KRS entity tables by number, never by name; no other identifier, no names and no free text are kept. Entries
   are notifications of a suspected offence filed by the KNF, not findings of guilt. #}
select
    section_key,
    lp,
    section_title,
    identifier_kind,
    krs_numbers,
    krs_count,
    prosecutor_office,
    prosecutor_level,
    final_ruling_mentioned,
    snapshot_date as date_key,
    'knf_warnings' as source_system,
    source_version,
    raw_sha256
from {{ ref('knf_warnings_entries') }}
qualify rank() over (order by ingested_at desc) = 1
