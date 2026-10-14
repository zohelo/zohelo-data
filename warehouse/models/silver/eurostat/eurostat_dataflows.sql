{# One row per Eurostat dataset: the dataflow of its newest structure message (name and
   description in English, data structure reference, update annotations). #}
with newest as (
    select dataset_id, arg_max(raw_sha256, cast(ingested_at as varchar) || raw_sha256) as raw_sha256
    from {{ source('eurostat', 'eurostat_structure_items') }}
    where artefact = 'dataflow'
    group by dataset_id
)
select
    i.dataset_id,
    i.artefact_id as dataflow_id,
    i.agency_id,
    i.artefact_version as dataflow_version,
    json_extract_string(i.names_json, '$.en') as name_en,
    json_extract_string(i.descriptions_json, '$.en') as description_en,
    json_extract_string(i.references_json, '$.Structure.id') as data_structure_id,
    json_extract_string(i.references_json, '$.Structure.version') as data_structure_version,
    (select max(json_extract_string(a, '$.title')) from (select unnest(cast(i.annotations_json as json[])) as a)
      where json_extract_string(a, '$.type') = 'UPDATE_DATA') as last_data_update,
    (select max(json_extract_string(a, '$.title')) from (select unnest(cast(i.annotations_json as json[])) as a)
      where json_extract_string(a, '$.type') = 'UPDATE_STRUCTURE') as last_structure_update,
    i.source_version,
    i.raw_sha256
from {{ source('eurostat', 'eurostat_structure_items') }} i
join newest n using (dataset_id, raw_sha256)
where i.artefact = 'dataflow'
