{# Eurostat code lists, one row per (agency, code list, version, code), deduplicated across
   the per-dataset structure messages that repeat them; English and native labels. #}
select
    agency_id,
    artefact_id as codelist_id,
    artefact_version as codelist_version,
    item_id as code,
    any_value(parent_id) as parent_code,
    any_value(json_extract_string(names_json, '$.en')) as name_en,
    any_value(json_extract_string(names_json, '$.de')) as name_de,
    any_value(json_extract_string(names_json, '$.fr')) as name_fr,
    count(distinct dataset_id) as dataset_count
from {{ source('eurostat', 'eurostat_structure_items') }}
where artefact = 'code'
group by all
