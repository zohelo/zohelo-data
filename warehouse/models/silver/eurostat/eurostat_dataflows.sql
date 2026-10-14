{# One row per Eurostat dataflow (= dataset): the newest published definition across the
   bulk dataflow messages and the per-dataset structure messages — English name and
   description, data structure reference, update annotations. #}
with flows as (
    select
        *,
        row_number() over (partition by artefact_id order by ingested_at desc, raw_sha256 asc) as newest
    from {{ source('eurostat', 'eurostat_structure_items') }}
    where artefact = 'dataflow'
)

select
    artefact_id as dataset_id,
    agency_id,
    artefact_version as dataflow_version,
    json_extract_string(names_json, '$.en') as name_en,
    json_extract_string(descriptions_json, '$.en') as description_en,
    json_extract_string(references_json, '$.Structure.id') as data_structure_id,
    json_extract_string(references_json, '$.Structure.version') as data_structure_version,
    (
        select max(json_extract_string(annotations.a, '$.title'))
        from (select unnest(cast(annotations_json as json[])) as a) as annotations
        where json_extract_string(annotations.a, '$.type') = 'UPDATE_DATA'
    ) as last_data_update,
    (
        select max(json_extract_string(annotations.a, '$.title'))
        from (select unnest(cast(annotations_json as json[])) as a) as annotations
        where json_extract_string(annotations.a, '$.type') = 'UPDATE_STRUCTURE'
    ) as last_structure_update,
    dataset_id as landing_entity,
    raw_sha256
from flows
where newest = 1
