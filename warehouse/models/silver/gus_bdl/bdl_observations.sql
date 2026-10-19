{{ config(materialized='incremental', incremental_strategy='append',
          full_refresh=('bdl_observations' in var('full_refresh_models', [])) or none,
          pre_hook="{% if is_incremental() %}{{ bdl_observations_clear() }}{% endif %}") }}
{# BDL observations, newest complete version of each subgroup, typed: unit code and name
   as exported, year, value (decimal comma → point; empty → null), measure unit and
   attribute as published, unit_code_scheme (12 characters = BDL unit id, 7 = TERYT TERC
   code as some exports publish), and the API variable id resolved from the subgroup and its
   dimension labels (website exports carry labels only; API refreshes carry the id).
   Incremental per Bronze file (owner policy #822): a row's recipe is its subgroup, its
   Landing file (raw_sha256) and variables_key, a digest of the subgroup's variable mapping
   (bdl_variables). The pre-hook deletes rows whose recipe is no longer current (superseded
   file or changed mapping); the build then inserts the current recipes Silver lacks, so a
   failed run is repaired by the next one. A change to this model's code makes the scheduled
   build rebuild it in full once (warehouse/changed.py). Recipe rule: macros/bdl_observations.sql. #}
with {{ bdl_current_recipes() }},

new_recipes as (
    select t.*
    from current_recipes as t
    {% if is_incremental() %}
        where
            t.subgroup_id || '|' || t.raw_sha256 || '|' || t.variables_key not in (
                select distinct h.subgroup_id || '|' || h.raw_sha256 || '|' || h.variables_key
                from {{ this }} as h
            )
    {% endif %}
),

rows as (
    select
        o.*,
        n.variables_key
    from {{ source('gus_bdl', 'bdl_subgroup_observations') }} as o
    inner join new_recipes as n on o.subgroup_id = n.subgroup_id and o.raw_sha256 = n.raw_sha256
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
    r.raw_sha256,
    r.variables_key
from rows as r
left join {{ ref('bdl_variables') }} as v
    on r.subgroup_id = v.subgroup_id and r.dimension_values = v.dimension_key
