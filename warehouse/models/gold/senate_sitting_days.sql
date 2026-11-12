{{ config(alias='senate_sitting_days') }}
{# Gold entity table (register source, kpi false; docs/model.md "Fact kinds": events): one row per sitting day of the
   Senate of the Republic of Poland, term XI (route D36), keyed by the Senate's own sitting id and the day number;
   date_key = the day's date (joins dim_calendar). Planned sittings are not here. Counts by month and year are in
   gold.build_senat. Latest version only (policy #822/#845). #}
select
    sitting_id,
    sitting_number,
    day_number,
    day_date as date_key,
    status_text,
    is_finished,
    day_page_landed,
    votes_listed,
    'senat' as source_system,
    source_version,
    raw_sha256
from {{ ref('senat_sitting_days') }}
