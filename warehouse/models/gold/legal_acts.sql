{{ config(alias='legal_acts') }}
{# Gold entity table (documents; docs/model.md "Fact kinds"): one row per act published in Dziennik Ustaw or Monitor
   Polski (register PLG-026), keyed by its ELI identifier (journal / year / [issue,] position); date_key = the promulgation
   date (empty when the API gives none). Title, type and status as published; metadata only, no act texts. #}
select
    eli_id,
    publisher,
    year,
    position,
    volume,
    address,
    display_address,
    promulgation_date as date_key,
    announcement_date,
    title,
    act_type,
    status,
    has_text_pdf,
    has_text_html,
    changed_at,
    'eli_pl' as source_system,
    source_version,
    raw_sha256
from {{ ref('eli_pl_acts') }}
