{# BDL observations, newest complete version of each subgroup, typed: unit code and name
   as exported, year, value (decimal comma → point; empty → null), measure unit and
   attribute as published, unit_code_scheme (12 characters = BDL unit id, 7 = TERYT TERC
   code as some exports publish), and the API variable id resolved from the subgroup and its
   dimension labels (website exports carry labels only; API refreshes carry the id). #}
with files as (
    select distinct
        subgroup_id,
        source_version,
        ingested_at,
        part_index,
        raw_sha256
    from {{ source('gus_bdl', 'bdl_subgroup_observations') }}
),

current_files as (
    select raw_sha256
    from files
    qualify
        source_version = max(source_version) over (partition by subgroup_id)
        and row_number() over (
            partition by subgroup_id, source_version, part_index
            order by ingested_at desc, raw_sha256 asc
        ) = 1
),

rows as (
    select o.*
    from {{ source('gus_bdl', 'bdl_subgroup_observations') }} as o
    where o.raw_sha256 in (select current_files.raw_sha256 from current_files)
)

select
    r.subgroup_id,
    coalesce(r.variable_id, v.variable_id) as variable_id,
    r.unit_code,
    case length(r.unit_code) when 12 then 'BDL' when 7 then 'TERC' end as unit_code_scheme,
    r.unit_name,
    try_cast(r.year as integer) as year,
    try_cast(replace(nullif(trim(r.value_text), ''), ',', '.') as double) as value,
    r.value_text,
    r.measure_unit,
    r.attribute,
    r.dimension_values,
    r.channel,
    r.source_version,
    r.raw_sha256
from rows as r
left join {{ ref('bdl_variables') }} as v
    on r.subgroup_id = v.subgroup_id and r.dimension_values = v.dimension_key
