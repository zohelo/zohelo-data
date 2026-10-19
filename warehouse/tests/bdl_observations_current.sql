{# silver.bdl_observations holds exactly the current recipes (incremental model, #822): every
   current Bronze file with its subgroup's variable mapping, and nothing else. A failure lists
   the recipes Silver lacks ("missing") or holds although not current ("stale"). #}
with {{ bdl_current_recipes() }},

held as (
    select distinct
        subgroup_id,
        raw_sha256,
        variables_key
    from {{ ref('bdl_observations') }}
)

select
    'missing' as problem,
    c.*
from current_recipes as c
where not exists (
    select 1 from held as h
    where h.subgroup_id = c.subgroup_id and h.raw_sha256 = c.raw_sha256 and h.variables_key = c.variables_key
)
union all
select
    'stale' as problem,
    h.*
from held as h
where not exists (
    select 1 from current_recipes as c
    where h.subgroup_id = c.subgroup_id and h.raw_sha256 = c.raw_sha256 and h.variables_key = c.variables_key
)
