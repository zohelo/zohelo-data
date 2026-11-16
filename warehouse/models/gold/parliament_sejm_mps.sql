{{ config(alias='parliament_sejm_mps') }}
{# Gold entity table (register): one row per MP of the Sejm's term 10 (register PLG-024), keyed by the API's MP id,
   allowlisted fields only: club code, constituency number and name, voivodeship, active flag (no names or other
   personal data). #}
select
    term,
    mp_id,
    is_active,
    club,
    district_num,
    district_name,
    voivodeship,
    'sejm' as source_system,
    source_version,
    raw_sha256
from {{ ref('sejm_mps') }}
