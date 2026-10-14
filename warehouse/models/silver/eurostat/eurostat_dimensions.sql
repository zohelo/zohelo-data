{# Dimensions of each Eurostat dataset: from the newest copy of the data structure its
   dataflow references — position, concept, and the code list (id, version) enumerating it. #}
with structures as (
    select
        *,
        dense_rank() over (partition by artefact_id, artefact_version order by ingested_at desc, raw_sha256) as newest
    from {{ source('eurostat', 'eurostat_structure_items') }}
    where artefact in ('dimension', 'time_dimension')
)

select
    f.dataset_id,
    s.item_id as dimension_id,
    try_cast(s.position as integer) as position,
    s.artefact = 'time_dimension' as is_time,
    json_extract_string(s.references_json, '$.ConceptIdentity.id') as concept_id,
    json_extract_string(s.references_json, '$.Enumeration.agencyID') as codelist_agency_id,
    json_extract_string(s.references_json, '$.Enumeration.id') as codelist_id,
    json_extract_string(s.references_json, '$.Enumeration.version') as codelist_version
from {{ ref('eurostat_dataflows') }} as f
inner join structures as s
    on s.artefact_id = f.data_structure_id and s.artefact_version = f.data_structure_version and s.newest = 1
