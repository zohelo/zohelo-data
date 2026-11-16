{{ config(alias='parliament_sejm_sitting_days') }}
{# Gold entity table (register source, kpi false; docs/model.md "Fact kinds": events): one row per sitting day of the
   Sejm, terms 9 and 10 (register PLG-024), keyed by the API's term, sitting number and the day; date_key = the
   day. is_held false for the planned later days of a running sitting. #}
select
    term,
    sitting_number,
    day_date as date_key,
    sitting_title,
    is_current_sitting,
    is_held,
    votings_listed,
    'sejm' as source_system,
    source_version,
    raw_sha256
from {{ ref('sejm_sitting_days') }}
