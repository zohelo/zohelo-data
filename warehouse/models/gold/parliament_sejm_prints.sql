{{ config(alias='parliament_sejm_prints') }}
{# Gold entity table (documents): one row per print (druk) of the Sejm, terms 9 and 10 (register PLG-024), keyed by
   the API's term and print number; date_key = the delivery date (empty when the API gives none). Title as
   published (metadata only, no print texts or attachments). #}
select
    term,
    print_number,
    delivery_date as date_key,
    document_date,
    changed_at,
    title,
    process_prints,
    additional_prints,
    'sejm' as source_system,
    source_version,
    raw_sha256
from {{ ref('sejm_prints') }}
