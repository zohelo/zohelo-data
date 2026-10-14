{# Dimensions of each Eurostat dataset (newest structure message): position, concept and
   the code list (id and version) that enumerates it. #}
with flows as (select dataset_id, raw_sha256, data_structure_id, data_structure_version from {{ ref('eurostat_dataflows') }})
select
    i.dataset_id,
    i.item_id as dimension_id,
    try_cast(i.position as integer) as position,
    i.artefact = 'time_dimension' as is_time,
    json_extract_string(i.references_json, '$.ConceptIdentity.id') as concept_id,
    json_extract_string(i.references_json, '$.Enumeration.agencyID') as codelist_agency_id,
    json_extract_string(i.references_json, '$.Enumeration.id') as codelist_id,
    json_extract_string(i.references_json, '$.Enumeration.version') as codelist_version
from {{ source('eurostat', 'eurostat_structure_items') }} i
join flows f on f.dataset_id = i.dataset_id and f.raw_sha256 = i.raw_sha256
    and i.artefact_id = f.data_structure_id and i.artefact_version = f.data_structure_version
where i.artefact in ('dimension', 'time_dimension')
