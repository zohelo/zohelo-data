{# BDL variables (API metadata), newest landed version per subgroup: variable id, the
   dimension labels n1–n5 and their JSON form as in the website exports (dimension_key),
   level, measure unit. #}
with ranked as (
    select
        *,
        max(ingested_at) over (partition by subgroup_id) as newest
    from {{ source('gus_bdl', 'bdl_metadata_variables') }}
)

select
    subgroup_id,
    try_cast(variable_id as bigint) as variable_id,
    n1,
    n2,
    n3,
    n4,
    n5,
    to_json(list_filter([n1, n2, n3, n4, n5], lambda x: x is not null))::varchar as dimension_key,
    try_cast(level as integer) as level,
    measure_unit_id,
    measure_unit_name,
    source_version,
    raw_sha256
from ranked
where ingested_at = newest
qualify row_number() over (partition by subgroup_id, variable_id order by raw_sha256) = 1
