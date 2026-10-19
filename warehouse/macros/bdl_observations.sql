{# Incremental GUS BDL Silver (models/silver/gus_bdl/bdl_observations.sql, owner policy #822).
   current_recipes: one row per current Bronze file — per subgroup the highest source_version
   and per part of it the newest ingest (the rule ingestion/common/latest.py keep_bdl prunes
   Bronze by) — with variables_key, a digest of the subgroup's variable mapping (dimension
   labels → variable id) that resolves its rows. #}
{% macro bdl_current_recipes() -%}
files as (
    select distinct
        subgroup_id,
        source_version,
        ingested_at,
        part_index,
        raw_sha256
    from {{ source('gus_bdl', 'bdl_subgroup_observations') }}
),

current_files as (
    select
        subgroup_id,
        raw_sha256
    from files
    qualify
        source_version = max(source_version) over (partition by subgroup_id)
        and row_number() over (
            partition by subgroup_id, source_version, part_index
            order by ingested_at desc, raw_sha256 asc
        ) = 1
),

variable_keys as (
    select
        subgroup_id,
        md5(string_agg(
            dimension_key || '=' || coalesce(cast(variable_id as varchar), ''), '|'
            order by dimension_key, variable_id
        )) as variables_key
    from {{ ref('bdl_variables') }}
    group by subgroup_id
),

current_recipes as (
    select
        f.subgroup_id,
        f.raw_sha256,
        coalesce(k.variables_key, '') as variables_key
    from current_files as f
    left join variable_keys as k on f.subgroup_id = k.subgroup_id
)
{%- endmacro %}

{# Pre-hook: delete the rows whose recipe is no longer current. #}
{% macro bdl_observations_clear() -%}
  delete from {{ this }}
  where subgroup_id || '|' || raw_sha256 || '|' || variables_key not in (
      with {{ bdl_current_recipes() }}
      select c.subgroup_id || '|' || c.raw_sha256 || '|' || c.variables_key from current_recipes as c
  )
{%- endmacro %}
