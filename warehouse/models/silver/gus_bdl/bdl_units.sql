{# BDL territorial units (API metadata), newest landed copy of each unit: 12-character
   unit id (KTS-based), name, parent, level 0–6, kind. #}
select
    unit_id,
    name,
    parent_id,
    try_cast(level as integer) as level,
    kind,
    has_description,
    source_version,
    raw_sha256
from {{ source('gus_bdl', 'bdl_metadata_units') }}
qualify row_number() over (partition by unit_id order by ingested_at desc, raw_sha256 asc) = 1
