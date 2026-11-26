{# Sejm API (register PLG-024): MPs of term 10, allowlisted fields only (reduced before Landing: no names, birth
   data, e-mails, professions or photos). Grain: term x mp_id (the API's MP id). club as the API codes it,
   district_num and district_name of the constituency (okręg wyborczy), voivodeship as published. #}
select
    cast(term as integer) as term,
    cast(mp_id as integer) as mp_id,
    is_active = 'true' as is_active,
    club,
    cast(district_num as integer) as district_num,
    district_name,
    voivodeship,
    source_version,
    raw_sha256
from {{ source('sejm', 'sejm_mps') }}
